package redbytes

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

func TestIntegrationWriterDirect(t *testing.T) {
	testCases := []struct {
		name  string
		src   func() io.Reader
		max   int
		parts []string
		pubX  int
	}{
		{
			"remainder",
			func() io.Reader {
				return bytes.NewBufferString(alphabet)
			},
			10,
			[]string{"abcdefghij", "klmnopqrst", "uvwxyz"},
			2, // 3 chunk in one go, plus close
		},
		{
			"exact multiple",
			func() io.Reader {
				return bytes.NewBuffer([]byte(alphabet)[1:])
			},
			5,
			[]string{"bcdef", "ghijk", "lmnop", "qrstu", "vwxyz"},
			2, // 5 chunk in one go, plus close
		},
		{
			"zero length",
			func() io.Reader {
				return bytes.NewBufferString("")
			},
			DefaultMaxChunkSize,
			nil,
			1, // just close
		},
		{
			"single bytes",
			func() io.Reader {
				return chunkReader(bytes.NewBufferString(alphabet), 1)
			},
			DefaultMaxChunkSize,
			[]string{
				"a", "b", "c", "d", "e", "f", "g", "h",
				"i", "j", "k", "l", "m", "n", "o", "p",
				"q", "r", "s", "t", "u", "v", "w", "x",
				"y", "z",
			},
			27, // individual chunks, plus close
		},
	}

	setTTL := 2 + int(time.Now().UnixNano()%7)
	durTTL := time.Second * time.Duration(setTTL)
	maxPTTL := setTTL * 1000

	base := randomString()
	for client, fetcher := range fetchers(t) {
		t.Run(client, func(t *testing.T) {
			for ix, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {

					name := fmt.Sprintf("%d-%s-%s", ix, client, base)
					meta := fmt.Sprintf("%s-%s", name, randomString())

					conn := fetcher.fetch()
					defer conn.Close()

					pubN := 0
					pub := doerFunc(func(cmd string, args ...interface{}) (interface{}, error) {
						if cmd == "PUBLISH" && args[0] == meta && args[1] == name {
							pubN++
						}
						return "0", nil
					})

					// function under test
					wc, err := NewRedisByteStreamWriter(
						context.Background(),
						conn,
						name,
						Expires(durTTL),
						Metadata([]byte(meta)),
						MaxChunkSize(tc.max),
						PublishChannel(meta),
						PublishClient(pub),
					)
					if err != nil {
						t.Fatalf("unexpected startup: %v\n", err)
					}

					_, err = io.Copy(wc, tc.src())
					if err != nil {
						t.Fatalf("unexpected copy error: %v\n", err)
					}

					err = wc.Close()
					if err != nil {
						t.Fatalf("unexpected close error: %v\n", err)
					}

					// Check on what was written to Redis
					expectations := map[string]string{
						metadata: meta,
						chunkEnd: strconv.Itoa(len(tc.parts) - 1),
					}
					for ix, part := range tc.parts {
						field := fmt.Sprintf(chunkFmt, ix)
						expectations[field] = part
					}

					var val string
					for field, should := range expectations {
						val, err = redis.String(conn.Do("HGET", name, field))
						if err != nil {
							t.Errorf("unexpected: %q %v\n", field, err)
						}
						if val != should {
							t.Errorf("%q; expected %q; got %q\n", field, should, val)
						}
					}
					if t.Failed() {
						contents, cerr := redis.StringMap(conn.Do("HGETALL", name))
						if cerr != nil {
							t.Fatalf("unexpected reporting failure: %v\n", cerr)
						}
						t.Logf("full contents of %q:\n", name)
						for k, v := range contents {
							t.Logf("%q: %q\n", k, v)
						}
					}

					pttl, err := redis.Int(conn.Do("PTTL", name))
					if err != nil {
						t.Fatalf("unexpected: %v\n", err)
					}
					if pttl >= maxPTTL {
						t.Errorf("expected PTTL < %d; got %d\n", maxPTTL, pttl)
					}

					if pubN != tc.pubX {
						t.Errorf("expected %d PUBLISH commands; got %d\n", tc.pubX, pubN)
					}

					// EOL state expectations
					_, err = wc.Write([]byte("postfacto"))
					if err != io.ErrClosedPipe {
						t.Errorf("expected %q; got %v\n", io.ErrClosedPipe, err)
					}
				})
			}
		})
	}
}

func TestIntegrationWriteReserveErrors(t *testing.T) {
	testCases := []struct {
		name  string
		setup func(conn redis.Conn, name string) error
	}{
		{
			"different datastructure",
			func(conn redis.Conn, name string) error {
				_, err := conn.Do("LPUSH", name, "")
				return err
			},
		},
		{
			"collision hash",
			func(conn redis.Conn, name string) error {
				_, err := conn.Do("HSET", name, "bubba", "gump")
				return err
			},
		},
	}

	base := randomString()
	for client, fetcher := range fetchers(t) {
		t.Run(client, func(t *testing.T) {
			for ix, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					name := fmt.Sprintf("%d-%s-%s", ix, client, base)

					conn := fetcher.fetch()
					defer conn.Close()
					defer func() {
						conn.Do("DELETE", name)
					}()

					err := tc.setup(conn, name)
					if err != nil {
						t.Errorf("unexpected: %v\n", err)
					}

					wc, err := NewRedisByteStreamWriter(context.Background(), conn, name)
					if err == nil {
						t.Errorf("expected an error on NewRedisByteStreamReader")
						wc.Close()
					}
				})
			}
		})
	}
}

func TestWriteReserveErrors(t *testing.T) {
	// Though similar to the Integration test version, it's easier
	// to simulate the race-condition error via a fake Doer
	key := randomString()
	info := randomString()

	testCases := []struct {
		name string
		doer Doer
		err  string
	}{
		{
			"data type conflict",
			doerFunc(func(cmd string, args ...interface{}) (interface{}, error) {
				if cmd == "HLEN" && args[0] == key {
					return nil, fmt.Errorf("conflict")
				}
				panic("should not get here")
			}),
			"conflict",
		},
		{
			"hash collision",
			doerFunc(func(cmd string, args ...interface{}) (interface{}, error) {
				if cmd == "HLEN" && args[0] == key {
					return []byte("1"), nil
				}
				panic("should not get here")
			}),
			ErrKeyExists.Error(),
		},
		{
			"flaky connection",
			doerFunc(func(cmd string, args ...interface{}) (interface{}, error) {
				if args[0] == key {
					if cmd == "HLEN" {
						return []byte("0"), nil
					}
					return nil, fmt.Errorf("flaky")
				}
				panic("should not get here")
			}),
			"flaky",
		},
		{
			"lost race",
			doerFunc(func(cmd string, args ...interface{}) (interface{}, error) {
				if args[0] == key {
					if cmd == "HLEN" {
						return []byte("0"), nil
					}
					return int64(0), nil
				}
				panic("should not get here")
			}),
			ErrKeyExists.Error(),
		},

	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := reserve(tc.doer, key, []byte(info))
			if err == nil || !strings.Contains(err.Error(), tc.err) {
				t.Errorf("expected %q; got %v\n", tc.err, err)
			}
		})
	}
}
