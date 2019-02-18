package redbytes

import (
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
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
			cm["redigo"] = pool
			fm["redigo"] = fetcherFunc(func() redis.Conn {
				return pool.Get()
			})
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

		resp, err = redis.Bytes(conn.Do("PING", time.Now().UnixNano()))
		t.Logf("PING: %v / %s", err, resp)
	}
}
