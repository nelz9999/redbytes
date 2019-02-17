package redbytes

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/gomodule/redigo/redis"
)

// WriteOptions hold the conditional aspects of setup for
// NewRedisByteStreamWriter
type WriteOptions struct {
	info     []byte
	maxChunk int
	expires  int
	pubChan  string
	pubDoer  Doer
}

// WriteOption sets up some of the conditional behavior associated
// with writing a stream of bytes to Redis
type WriteOption func(wo *WriteOptions) error

// Metadata stores arbitrary data with the stream of bytes being stored.
// For example, if you are using this in an HTTP context, maybe you want
// to know the mime type of the file? If not set, an empty value is used.
func Metadata(info []byte) WriteOption {
	return func(wo *WriteOptions) error {
		wo.info = info
		return nil
	}
}

// MaxChunkSize sets the upper bound of how "wide" a single chunck can be
// when written to Redis as a hash field. This has implications on the
// read-side - if the read buffers are smaller than what is stored
// in a single field, they may receive an io.ErrShortBuffer.
// If not set, the value used is DefaultMaxChunkSize.
func MaxChunkSize(max int) WriteOption {
	return func(wo *WriteOptions) error {
		if max <= 0 {
			return fmt.Errorf("max chunk size must be >0: %d", max)
		}
		wo.maxChunk = max
		return nil
	}
}

// Expires sets the given stream of data to expire out of Redis after the
// given amount of time (rounded to the nearest second).
// If not set, the stream will be written without an expiry time.
func Expires(d time.Duration) WriteOption {
	return func(wo *WriteOptions) error {
		seconds := int(d.Round(time.Second).Seconds())
		if seconds <= 0 {
			return fmt.Errorf("expiry seconds must be >0: %d", seconds)
		}
		wo.expires = seconds
		return nil
	}
}

// PublishChannel sets the Redis PubSub channel where notifications (with
// bodies of zero length) are sent to signify that new data is available
// in the stream. If not set, no PubSub notifications are emitted.
func PublishChannel(channel string) WriteOption {
	return func(wo *WriteOptions) error {
		wo.pubChan = channel
		return nil
	}
}

// PublishClient allows the caller to set a different Redis client for sending
// PubSub messages. This is probably only needed for Redis Cluster users,
// when the PublishChannel might get mapped to a different node than the
// base hash key. If not set, the initial Redis client will be used
// by default.
func PublishClient(client Doer) WriteOption {
	return func(wo *WriteOptions) error {
		if client == nil {
			return fmt.Errorf("publish client must not be nil")
		}
		wo.pubDoer = client
		return nil
	}
}

// NewRedisByteStreamWriter returns an io.WriteCloser that stores the written
// byte stream as a hash in Redis at the given key. This allows a receiver
// on the other end to get access to the data before the whole stream
// is finished being written, even if the write-side takes a while to finish.
// The Redis client is not closed upon call to the Close() method, so it
// should be safe to re-use the client afterwards, as well as concurrently
// (as long as no other operations are blocking on the client, like for
// a Subscribe/PSubscribe).
func NewRedisByteStreamWriter(ctx context.Context, client Doer, key string, opts ...WriteOption) (io.WriteCloser, error) {
	// Default values
	wo := &WriteOptions{
		maxChunk: DefaultMaxChunkSize,
		pubDoer:  client,
	}

	// Set conditional configuration
	var err error
	for _, opt := range opts {
		err = opt(wo)
		if err != nil {
			return nil, err
		}
	}

	// Jealously reserve our space in the datastore
	err = reserve(client, key, wo.info)
	if err != nil {
		return nil, err
	}

	// Basic writer
	wc := newStreamToHashWriteCloser(ctx, client, key, wo.maxChunk)

	// Add the expires writer if configured
	if wo.expires > 0 {
		wc = newExpiresWriteCloser(wc, client, key, wo.expires)
	}

	// Add the pubsub publisher if configured
	if wo.pubChan != "" {
		wc = newPublishesWriteCloser(wc, wo.pubDoer, wo.pubChan, key)
	}

	// Send it off into the world to do its work
	return wc, nil
}

func reserve(doer Doer, key string, info []byte) error {
	size, err := redis.Int(doer.Do("HLEN", key))
	if err != nil {
		// This case catches connection issues, as well as if
		// the key exists, but is of a different type than a hash.
		return err
	}
	if size != 0 {
		// This catches the case where the hash already exists
		return ErrKeyExists
	}

	// Plant our flag that we want to own this key and hash field
	ok, err := redis.Bool(doer.Do("HSETNX", key, metadata, info))
	if err != nil {
		return err
	}
	if !ok {
		// This is the case where we are racing against another
		// redbytes writer, and we have lost the race.
		return ErrKeyExists
	}
	return nil
}

type closureWriteCloser struct {
	wfunc func(p []byte) (int, error)
	cfunc func() error
}

func (cwc closureWriteCloser) Write(p []byte) (int, error) {
	return cwc.wfunc(p)
}

func (cwc closureWriteCloser) Close() error {
	return cwc.cfunc()
}

func newStreamToHashWriteCloser(ctx context.Context, doer Doer, key string, chunkMax int) io.WriteCloser {
	chunkNum := 0
	return closureWriteCloser{
		wfunc: func(p []byte) (int, error) {
			if ctx.Err() != nil {
				return 0, ctx.Err()
			}

			size := len(p)
			if size == 0 {
				return 0, nil
			}

			offset := 0
			fvs := []interface{}{key}
			for offset < size {
				diff := size - offset
				if diff > chunkMax {
					diff = chunkMax
				}
				fvs = append(fvs, fmt.Sprintf(chunkFmt, chunkNum), p[offset:offset+diff])
				offset += diff
				chunkNum++
			}
			_, err := doer.Do("HMSET", fvs...)
			if err != nil {
				return 0, err
			}
			return size, nil
		},
		cfunc: func() error {
			end := chunkNum - 1
			_, err := doer.Do("HSET", key, chunkEnd, fmt.Sprintf("%d", end))
			return err
		},
	}
}

func newExpiresWriteCloser(base io.WriteCloser, doer Doer, key string, seconds int) io.WriteCloser {
	return closureWriteCloser{
		wfunc: func(p []byte) (int, error) {
			n, err := base.Write(p)
			if err != nil || n == 0 {
				return n, err
			}
			_, err = doer.Do("EXPIRE", key, seconds)
			return n, err
		},
		cfunc: func() error {
			err := base.Close()
			if err != nil {
				return err
			}
			_, err = doer.Do("EXPIRE", key, seconds)
			return err
		},
	}
}

// newPublishesWriteCloser publish the key onto the given channel. This means
// the caller could be listening on a single channel for multiple streams.
func newPublishesWriteCloser(base io.WriteCloser, doer Doer, channel, key string) io.WriteCloser {
	return closureWriteCloser{
		wfunc: func(p []byte) (int, error) {
			n, err := base.Write(p)
			if err != nil || n == 0 {
				return n, err
			}
			_, err = doer.Do("PUBLISH", channel, key)
			return n, err
		},
		cfunc: func() error {
			err := base.Close()
			if err != nil {
				return err
			}
			_, err = doer.Do("PUBLISH", channel, key)
			return err
		},
	}
}

func newClosedPipeWriteCloser(base io.WriteCloser) io.WriteCloser {
	closed := false
	errored := false
	return closureWriteCloser{
		wfunc: func(p []byte) (int, error) {
			if errored || closed {
				return 0, io.ErrClosedPipe
			}
			n, err := base.Write(p)
			if err != nil {
				errored = true
			}
			return n, err
		},
		cfunc: func() error {
			// In the case of a Write(...) error, we still want to
			// allow through a close. The thinking goes this way:
			// - Not even attempting to close the data stream
			// 		would lead to starvations on read side.
			// - If it's a long-lasting connection error, we'd be
			// 		messed up either way
			// - If it's an intermittent connection error, then closing
			// 		out the data would be a nice signal to the read side that
			// 		the stream has ended, yet we've still protected ourselves
			// 		from writing data out-of-order.
			if closed {
				return nil
			}
			closed = true
			return base.Close()
		},
	}
}
