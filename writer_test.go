package redbytes

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

func TestIntegrationWriter(t *testing.T) {
	testCases := []struct {
		name  string
		src   io.Reader
		max   int
		parts []string
	}{
		{
			"remainder",
			bytes.NewBufferString(alphabet),
			10,
			[]string{"abcdefghij", "klmnopqrst", "uvwxyz"},
		},
		{
			"exact multiple",
			bytes.NewBuffer([]byte(alphabet)[1:]),
			5,
			[]string{"bcdef", "ghijk", "lmnop", "qrstu", "vwxyz"},
		},
		{
			"zero length",
			bytes.NewBufferString(""),
			DefaultMaxChunkSize,
			nil,
		},
		{
			"single bytes",
			chunkReader(bytes.NewBufferString(alphabet), 1),
			DefaultMaxChunkSize,
			[]string{
				"a", "b", "c", "d", "e",
				"f", "g", "h", "i", "j",
				"k", "l", "m", "n", "o",
				"p", "q", "r", "s", "t",
				"u", "v", "w", "x", "y",
				"z",
			},
		},
	}

	setTTL := 2 + int(time.Now().UnixNano()%7)
	durTTL := time.Second * time.Duration(setTTL)
	maxPTTL := setTTL * 1000

	fm := fetchers(t)
	base := randomString()
	for client, fetcher := range fm {
		t.Run(client, func(t *testing.T) {
			for ix, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {

					name := fmt.Sprintf("%d-%s-%s", ix, client, base)
					meta := fmt.Sprintf("%s-%s", name, randomString())

					conn := fetcher.fetch()
					defer conn.Close()

					// function under test
					wc, err := NewRedisByteStreamWriter(
						context.Background(),
						conn,
						name,
						Expires(durTTL),
						Metadata([]byte(meta)),
						MaxChunkSize(tc.max),
					)
					if err != nil {
						t.Fatalf("unexpected: %v\n", err)
					}

					_, err = io.Copy(wc, tc.src)
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

					hasErr := false
					var val string
					for field, should := range expectations {
						val, err = redis.String(conn.Do("HGET", name, field))
						if err != nil {
							hasErr = true
							t.Errorf("unexpected: %v\n", err)
						}
						if val != should {
							hasErr = true
							t.Errorf("%q; expected %q; got %q\n", field, should, val)
						}
					}
					if hasErr {
						contents, err := redis.StringMap(conn.Do("HGETALL", name))
						if err != nil {
							t.Fatalf("unexpected: %v\n", err)
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
				})
			}
		})
	}
}
