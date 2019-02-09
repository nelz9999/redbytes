package redbytes

import (
	"fmt"
	"io"

	"github.com/gomodule/redigo/redis"
)

// WriteOptions hold the conditional aspects of setup for
// NewRedisByteStreamWriter
type WriteOptions struct {
	info     []byte
	noClean  bool
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

// DisableCollisionCleanup stops us from removing a field that may have
// been added to a hash that already exists at the key given to us
// by the caller. You probably don't want to use this, but it is provided
// for completeness.
func DisableCollisionCleanup() WriteOption {
	return func(wo *WriteOptions) error {
		wo.noClean = true
		return nil
	}
}

// MaxChunkSize sets the upper bound of how "wide" a single chunck can be
// when written to Redis as a hash field. If not set, the value used
// is DefaultMaxChunkSize.
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
// given amount of time. If not set, the stream will be written without
// an expiry time.
func Expires(seconds int) WriteOption {
	return func(wo *WriteOptions) error {
		if seconds < 0 {
			return fmt.Errorf("expiry seconds must be >=0: %d", seconds)
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
		wo.pubDoer = client
		return nil
	}
}

// NewRedisByteStreamWriter returns an io.WriteCloser that stores the written
// byte stream as a hash in Redis at the given key. This allows a receiver
// on the other end to get access to the data before the whole stream
// is finished being written, even if the write-side takes a long to finish.
// The given client is not closed upon call to the Close() method, so it
// should be safe to re-use the client afterwards, as well as concurrently
// (as long as no other operations are blocking on the client, like for
// a Subscribe/PSubscribe).
func NewRedisByteStreamWriter(client Doer, key string, opts ...WriteOption) (io.WriteCloser, error) {
	var err error

	wo := &WriteOptions{
		maxChunk: DefaultMaxChunkSize,
		pubDoer:  client,
	}

	for _, opt := range opts {
		err = opt(wo)
		if err != nil {
			return nil, err
		}
	}

	res := newBaseReserver()
	if !wo.noClean {
		res = newCleanupReserver(res)
	}

	_, err = res.Reserve(client, key, wo.info)
	if err != nil {
		return nil, err
	}

	wc := newStreamToHashWriteCloser(client, key, wo.maxChunk)
	if wo.expires > 0 {
		wc = newExpiresWriteCloser(wc, client, key, wo.expires)
	}
	if wo.pubChan != "" {
		wc = newPublishesWriteCloser(wc, wo.pubDoer, wo.pubChan)
	}

	return wc, nil
}

// reserver is a composable interface for claiming the key/hash
type reserver interface {
	Reserve(doer Doer, key string, info []byte) (int, error)
}

// reserverFunc is an adapter a function to conform to the reserver interface
type reserverFunc func(doer Doer, key string, info []byte) (int, error)

func (fn reserverFunc) Reserve(doer Doer, key string, info []byte) (int, error) {
	return fn(doer, key, info)
}

func newBaseReserver() reserver {
	return reserverFunc(func(doer Doer, key string, info []byte) (int, error) {
		// Plant our flag that we want to own this key and hash field
		ok, err := redis.Bool(doer.Do("HSETNX", key, metadata, info))
		if err != nil {
			// This case catches connection issues, as well as a key that
			// already exists but is of a different type than a hash.
			return 0, err
		}
		if !ok {
			// This is the case where we are racing against another
			// writer, and we have lost the race.
			return 0, ErrKeyExists
		}

		// Now, there is one last possibility to consider. We may have been
		// the first to write the key/field combo, but that doesn't protect
		// against us having written into some arbitrary other hash with
		// the same key, but different fields. If this happened, there will
		// be more than the one field.
		return redis.Int(doer.Do("HLEN", key))
	})
}

func newCleanupReserver(base reserver) reserver {
	return reserverFunc(func(doer Doer, key string, info []byte) (int, error) {
		count, err := base.Reserve(doer, key, info)
		if err != nil || count == 1 {
			return count, err
		}

		// If the caller gave us a bum steer, and we've now dropped
		// a new field into an existing hash, let's clean up our
		// mess before we bail out.
		_, err = doer.Do("HDEL", key, metadata)
		if err != nil {
			return count, err
		}
		return count, ErrKeyExists
	})
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

func newStreamToHashWriteCloser(doer Doer, key string, chunkMax int) io.WriteCloser {
	chunkNum := 0
	return closureWriteCloser{
		wfunc: func(p []byte) (int, error) {
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

func newPublishesWriteCloser(base io.WriteCloser, doer Doer, channel string) io.WriteCloser {
	return closureWriteCloser{
		wfunc: func(p []byte) (int, error) {
			n, err := base.Write(p)
			if err != nil || n == 0 {
				return n, err
			}
			_, err = doer.Do("PUBLISH", channel, "")
			return n, err
		},
		cfunc: func() error {
			err := base.Close()
			if err != nil {
				return err
			}
			_, err = doer.Do("PUBLISH", channel, "")
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
			// TODO: decide - in the case of a Write(...) error, do we want to
			// completely kill a trailing Close(), or only allow through
			// a single one? (
			// - Not even attempting to close the data stream
			// 		would likely lead to starvations on read side.
			// - If it's a long-lasting connection error, we'd be
			// 		busted up either way
			// - If it's an intermittent connection error, then closing
			// 		out the data would be a nice signal to the read side that
			// 		the stream has ended, and we've still protected ourselves
			// 		from writing data out-of-order in the Write(...)
			if closed {
				return nil
			}
			closed = true
			return base.Close()
		},
	}
}
