package redbytes

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"
	"time"
)

func chunkReader(r io.Reader, max int) io.Reader {
	return readerFunc(func(p []byte) (int, error) {
		size := len(p)
		if size > max {
			size = max
		}
		return r.Read(p[:size])
	})
}

func TestIntegrationReaderDirect(t *testing.T) {
	testCases := []struct {
		name string
		vals []string
	}{
		{
			"single",
			[]string{alphabet},
		},
		{
			"several",
			[]string{"abcdefghij", "klmnopqrst", "uvwxyz"},
		},
		{
			"bytewise",
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

	var err error
	base := randomString()
	for client, fetcher := range fetchers(t) {
		t.Run(client, func(t *testing.T) {
			for ix, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					name := fmt.Sprintf("%d-%s-%s", ix, client, base)
					meta := fmt.Sprintf("%s-%s", name, randomString())

					conn := fetcher.fetch()
					defer conn.Close()

					// We just directly put the data directly to Redis,
					// like Write would.
					_, err = conn.Do(
						"HMSET", name,
						metadata, meta,
						chunkEnd, (len(tc.vals) - 1),
					)
					if err != nil {
						t.Fatalf("unexpected: %v\n", err)
					}
					for ix, val := range tc.vals {
						_, err = conn.Do(
							"HSET", name,
							fmt.Sprintf(chunkFmt, ix), val,
						)
						if err != nil {
							t.Fatalf("unexpected: %v\n", err)
						}
					}

					// Function under test
					md, rc, err := NewRedisByteStreamReader(
						context.Background(),
						conn,
						name,
						PollInterval(time.Second),
						Lookahead(10),
					)
					if err != nil {
						t.Fatalf("unexpected: %v\n", err)
					}
					defer rc.Close()

					dest := bytes.NewBufferString("")
					_, err = io.Copy(dest, rc)
					if err != nil {
						t.Fatalf("unexpected: %v\n", err)
					}

					// Did we get the result values we expected?
					if meta != string(md) {
						t.Errorf("metadata expected %q; got %q\n", meta, string(md))
					}
					if alphabet != dest.String() {
						t.Errorf("content expected %q; got %q\n", alphabet, dest.String())
					}
				})
			}
		})
	}
}
