package redbytes

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

// ReadOptions hold the conditional aspects of setup for
// NewRedisByteStreamReader
type ReadOptions struct {
	ahead   int
	poll    time.Duration
	psc     redis.PubSubConn
	channel string
	starve  time.Duration
}

// ReadOption sets up some of the conditional behavior associated
// with reading a stream of bytes from Redis
type ReadOption func(ro *ReadOptions) error

// Lookahead sets the number of optimistic forward-looking chunks to be
// requested upon a fetch from Redis. This can reduce the overall number
// of round-trips to Redis over the life of a stream. If not set, no
// lookahead is used.
func Lookahead(count int) ReadOption {
	return func(ro *ReadOptions) error {
		if count < 0 {
			return fmt.Errorf("lookahead must be >= 0: %d", count)
		}
		ro.ahead = count
		return nil
	}
}

// StarveInterval sets the maximum amount of time to wait between successful
// receipts of data on the stream, after which it gives up an returns
// ErrIncomplete. If not set, the DefaultStarveInterval will be used.
// If you are only using PollInterval as read stimulus, this value should
// be greater than that interval.
func StarveInterval(d time.Duration) ReadOption {
	return func(ro *ReadOptions) error {
		if d <= 0 {
			return fmt.Errorf("starve interval must be >= 0: %s", d)
		}
		ro.starve = d
		return nil
	}
}

// PollInterval sets a maximum period between requests for more data
// when waiting on the given stream.
// At least one of Subscribe(...) or PollInterval(...) MUST be configured
// as read stimulus, both are acceptable and can coexists.
func PollInterval(d time.Duration) ReadOption {
	return func(ro *ReadOptions) error {
		if d <= 0 {
			return fmt.Errorf("poll interval must be >= 0: %s", d)
		}
		ro.poll = d
		return nil
	}
}

// Subscribe sets up a Redis PubSub subscription to the given channel, and
// listens on this channel for notifications that more information may
// be available for our given stream. The given client MUST necessarily
// be a different instance than the client given to read information
// from the stream, as a SUBSCRIBE puts the client into a long-lived
// connection to the server.
// At least one of Subscribe(...) or PollInterval(...) MUST be configured
// as read stimulus, both are acceptable and can coexists.
func Subscribe(client redis.Conn, channel string) ReadOption {
	return func(ro *ReadOptions) error {
		if channel == "" {
			return fmt.Errorf("channel must be non-empty: %q", channel)
		}
		if client == nil {
			return fmt.Errorf("subscribe client must be non-nil")
		}
		ro.psc = redis.PubSubConn{Conn: client}
		ro.channel = channel
		return nil
	}
}

func NewRedisByteStreamReader(parent context.Context, doer Doer, key string, opts ...ReadOption) ([]byte, io.ReadCloser, error) {
	ro := &ReadOptions{
		starve: DefaultStarveInterval,
	}
	var err error
	for _, opt := range opts {
		err = opt(ro)
		if err != nil {
			return nil, nil, err
		}
	}

	closers := []func() error{}
	closeFn := func() error {
		var result error
		for _, closer := range closers {
			e := closer()
			if result == nil {
				result = e
			}
		}
		return result
	}

	var subs chan struct{}
	if ro.channel != "" {
		err = ro.psc.Subscribe(ro.channel)
		if err != nil {
			return nil, nil, err
		}
		var wg sync.WaitGroup
		closers = append(closers, func() error {
			cerr := ro.psc.Unsubscribe()
			wg.Wait()
			return cerr
		})

		subs = make(chan struct{})
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				switch n := ro.psc.Receive().(type) {
				case error: // TODO: consider ctx.cxl()?
					return
				case redis.Message:
					if string(n.Data) == key {
						subs <- struct{}{}
					}
				case redis.Subscription:
					if n.Count == 0 {
						return
					}
				}
			}
		}()
	}

	var tick <-chan time.Time
	if ro.poll != 0 {
		ticker := time.NewTicker(ro.poll)
		tick = ticker.C
		closers = append(closers, func() error {
			ticker.Stop()
			return nil
		})
	}

	if subs == nil && tick == nil {
		return nil, nil, ErrNoStimulus
	}

	ctx, cxl := context.WithCancel(parent)
	closers = append(closers, func() error {
		cxl()
		return nil
	})

	info, err := fetchInfo(ctx, doer, key, ro.starve, tick, subs)
	if err != nil {
		defer closeFn()
		return nil, nil, err
	}

	rr := newStreamFromHashReader(doer, key, ro.ahead)
	rr = newRetryReader(ctx, rr, ro.starve, tick, subs)
	rc := closeFuncReadCloser{rr, closeFn}

	return info, rc, nil
}

type readerFunc func(p []byte) (n int, err error)

func (fn readerFunc) Read(p []byte) (int, error) {
	return fn(p)
}

type closeFuncReadCloser struct {
	io.Reader
	fn func() error
}

func (c closeFuncReadCloser) Close() error {
	return c.fn()
}

func newStreamFromHashReader(doer Doer, key string, ahead int) io.Reader {
	count := 0
	final := -1
	return readerFunc(func(p []byte) (int, error) {
		if final > -1 && count > final {
			return 0, io.EOF
		}

		fields := []interface{}{key}
		if final == -1 {
			fields = append(fields, chunkEnd)
		}
		for ix := count; ix <= count+ahead; ix++ {
			fields = append(fields, fmt.Sprintf(chunkFmt, ix))
		}

		results, err := redis.ByteSlices(doer.Do("HMGET", fields...))
		if err != nil {
			return 0, err
		}

		if final == -1 {
			if len(results[0]) > 0 {
				val, err := strconv.Atoi(string(results[0]))
				if err != nil {
					// If we can't parse this, either we're doing something
					// wrong on the write-side, or somebody's mucking about
					// in our data!
					return 0, err
				}
				final = val
			}
			results = results[1:]
		}
		offset := 0
		for ix, data := range results {
			if len(data) == 0 {
				break
			}
			n := copy(p[offset:], data)
			if n < len(data) {
				if ix == 0 {
					// If we can't fit even the first chunk into the buffer,
					// this is a problem.
					return 0, io.ErrShortBuffer
				}
				break
			}
			// All the data fit in the buffer, if there are more chunks,
			// we can try to put them into the buffer as well.
			offset += n
			count++
		}
		if offset == 0 {
			if final != -1 && count > final {
				return 0, io.EOF
			}
			return 0, errIncomplete
		}
		return offset, nil
	})
}

func newRetryReader(ctx context.Context, base io.Reader, starve time.Duration, tick <-chan time.Time, subs <-chan struct{}) io.Reader {
	first := make(chan struct{})
	close(first)
	recent := time.Now()
	return readerFunc(func(p []byte) (int, error) {
		for {
			starveTTL := recent.Add(starve).Sub(time.Now())
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(starveTTL):
				return 0, io.ErrUnexpectedEOF
			case <-first:
				// we automatically want to try the underlying Reader
				// on our first time through
				first = nil
			case <-tick:
				// We received polling stimulus to try the read again
			case <-subs:
				// We received pubsub stimulus to try the read again
			}

			// Try the underlying Reader
			n, err := base.Read(p)
			if n > 0 {
				// Mark time of a successful read.
				// We do not check for err == nil, because the io.Reader
				// documentation states: "Callers should always process the
				// n > 0 bytes returned before considering the error err."
				recent = time.Now()
			}
			if err != errIncomplete {
				// We only want to loop/retry when we've gotten
				// the ErrIncomplete signal from the underlying Reader
				return n, err
			}
		}
	})
}

func fetchInfo(ctx context.Context, doer Doer, key string, starve time.Duration, tick <-chan time.Time, subs <-chan struct{}) ([]byte, error) {
	first := make(chan struct{})
	close(first)
	recent := time.Now()

	for {
		starveTTL := recent.Add(starve).Sub(time.Now())
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(starveTTL):
			return nil, io.ErrUnexpectedEOF
		case <-first:
			// we automatically want to try the underlying Reader
			// on our first time through
			first = nil
		case <-tick:
			// We received polling stimulus to try the read again
		case <-subs:
			// We received pubsub stimulus to try the read again
		}

		ok, err := redis.Bool(doer.Do("HEXISTS", key))
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}

		// We've found out that this field exists, so now we just
		// return that data!
		return redis.Bytes(doer.Do("HGET", key, metadata))
	}
}
