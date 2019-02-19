package redbytes

import (
	"context"
	"crypto/md5"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/mna/redisc"
)

func TestMain(m *testing.M) {
	flag.Parse()
	result := m.Run()
	for name, closer := range cm {
		err := closer.Close()
		fmt.Fprintf(os.Stdout, "shutdown %q - %v\n", name, err)
	}
	os.Exit(result)
}

const envRedisSingle = "REDIS_SINGLE"
const envRedisCluster = "REDIS_CLUSTER"

var testOnce sync.Once
var fm = map[string]fetcher{}
var cm = map[string]io.Closer{}

func fetchers(t *testing.T) map[string]fetcher {
	t.Helper()
	if testing.Short() {
		t.Skip("skip for -short")
	}
	testOnce.Do(func() {
		single := os.Getenv(envRedisSingle)
		if single != "" {
			t.Logf("env %q = %q\n", envRedisSingle, single)
			pool := &redis.Pool{
				MaxIdle:     3,
				IdleTimeout: 10 * time.Second,
				Dial: func() (redis.Conn, error) {
					return redis.Dial("tcp", single)
				},
				TestOnBorrow: func(c redis.Conn, _ time.Time) error {
					now := fmt.Sprintf("%d", time.Now().UnixNano())
					resp, err := redis.String(c.Do("PING", now))
					if err != nil {
						return err
					}
					if !strings.Contains(resp, now) {
						return fmt.Errorf("bad PING")
					}
					return nil
				},
			}
			_ = pool
			cm["redigo"] = pool
			fm["redigo"] = fetcherFunc(func() redis.Conn {
				return pool.Get()
			})
		}
		multi := os.Getenv(envRedisCluster)
		if multi != "" {
			t.Logf("env %q = %q\n", envRedisCluster, multi)
			cluster := &redisc.Cluster{
				StartupNodes: []string{multi},
				DialOptions:  []redis.DialOption{redis.DialConnectTimeout(2 * time.Second)},
				CreatePool: func(addr string, opts ...redis.DialOption) (*redis.Pool, error) {
					return &redis.Pool{
						MaxIdle:     3,
						IdleTimeout: 10 * time.Second,
						Dial: func() (redis.Conn, error) {
							return redis.Dial("tcp", addr, opts...)
						},
						TestOnBorrow: func(c redis.Conn, _ time.Time) error {
							_, err := c.Do("PING")
							return err
						},
					}, nil
				},
			}
			err := cluster.Refresh()
			if err != nil {
				t.Errorf("redisc: %v\n", err)
			} else {
				cm["redisc"] = cluster
				fm["redisc"] = fetcherFunc(func() redis.Conn {
					return cluster.Get()
				})
			}
		}
	})
	if len(fm) < 1 {
		t.Fatalf("neither %q or %q are defined\n", envRedisSingle, envRedisCluster)
	}
	return fm
}

type fetcher interface {
	fetch() redis.Conn
}

type fetcherFunc func() redis.Conn

func (fn fetcherFunc) fetch() redis.Conn {
	return fn()
}

type doerFunc func(cmd string, args ...interface{}) (interface{}, error)

func (fn doerFunc) Do(cmd string, args ...interface{}) (interface{}, error) {
	return fn(cmd, args...)
}

func TestIntegrationRedis(t *testing.T) {
	fm := fetchers(t)
	for name, pool := range fm {
		conn := pool.fetch()
		defer conn.Close()
		resp, err := redis.Bytes(conn.Do("INFO", "server"))
		if err != nil {
			t.Errorf("%q - %v\n", name, err)
			continue
		}
		t.Logf("%s: \n%s\n", name, resp)
	}
}

func TestIntegrationRoundtripSuccess(t *testing.T) {
	base := randomString()
	channel := fmt.Sprintf("my-pubsub-channel-name-%s", base)
	testCases := []struct {
		name  string
		wopts func() []WriteOption
		ropts func(c redis.Conn) []ReadOption
	}{
		{
			"no pubsub",
			func() []WriteOption {
				return nil
			},
			func(c redis.Conn) []ReadOption {
				return []ReadOption{
					PollInterval(75 * time.Millisecond),
					Lookahead(5),
					StarveInterval(500 * time.Millisecond),
				}
			},
		},
		{
			"ya pubsub",
			func() []WriteOption {
				return []WriteOption{PublishChannel(channel)}
			},
			func(c redis.Conn) []ReadOption {
				return []ReadOption{
					Subscribe(c, channel),
					Lookahead(5),
					StarveInterval(500 * time.Millisecond),
				}
			},
		},
	}

	for client, fetcher := range fetchers(t) {
		t.Run(client, func(t *testing.T) {
			for ix, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					name := fmt.Sprintf("%d-%s-%s", ix, client, base)
					meta := fmt.Sprintf("%s-%s", name, randomString())
					ctx, cxl := context.WithCancel(context.Background())
					defer cxl()

					size := (3 + (time.Now().UnixNano() % 7)) * 1024 // 3-9 kb
					// t.Logf("size: %d\n", size)

					// Read-side
					rCh := make(chan string)
					go func() {
						defer close(rCh)

						rx := fetcher.fetch()
						defer rx.Close()

						sub := fetcher.fetch()
						defer sub.Close()

						md, rc, err := NewRedisByteStreamReader(ctx, rx, name, tc.ropts(sub)...)
						if err != nil {
							t.Errorf("reader standup: %v\n", err)
							return
						}
						defer rc.Close()
						if string(md) != meta {
							t.Errorf("expected %q; got %q\n", meta, md)
						}

						sum := md5.New()
						n, err := io.Copy(sum, rc)
						if err != nil {
							t.Errorf("reader copy: %v\n", err)
							return
						}

						if n != size {
							t.Errorf("expected size %d; got %d\n", size, n)
						}

						rCh <- fmt.Sprintf("%x", sum.Sum(nil))
					}()

					// Write side
					wCh := make(chan string)
					go func() {
						defer close(wCh)

						tx := fetcher.fetch()
						defer tx.Close()

						wc, err := NewRedisByteStreamWriter(ctx, tx, name, append(tc.wopts(), Expires(time.Minute), Metadata([]byte(meta)))...)
						if err != nil {
							t.Errorf("writer standup: %v\n", err)
							return
						}
						defer wc.Close()

						src := rand.Reader
						src = io.LimitReader(src, size)             // 7kb as a good test?
						src = chunkReader(src, 150)                 // 100 bytes at a time
						src = delayReader(src, 50*time.Millisecond) // 50ms per chunk

						sum := md5.New()

						n, err := io.Copy(io.MultiWriter(wc, sum), src)
						if err != nil {
							t.Errorf("writer copy: %v\n", err)
							return
						}

						if n != size {
							t.Errorf("expected size %d; got %d\n", size, n)
						}

						wCh <- fmt.Sprintf("%x", sum.Sum(nil))
					}()

					rSum, wSum := "", ""
					select {
					case s := <-wCh:
						wSum = s
					case <-time.After(10 * time.Second):
						t.Errorf("write-side timeout")
					}

					select {
					case s := <-rCh:
						rSum = s
					case <-time.After(10 * time.Second):
						t.Errorf("read-side timeout")
					}

					if wSum != rSum {
						t.Errorf("mismatch checksums: %q != %q\n", wSum, rSum)
					}
					// t.Logf("checks: %q\n", wSum)
				})
			}
		})
	}
}
