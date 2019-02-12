package redbytes

import "time"

const (
	chunkFmt = "chunk:%d"
	chunkEnd = "chunk:end"
	metadata = "metadata"

	// DefaultMaxChunkSize is the base defined width of each data field
	DefaultMaxChunkSize = 1024

	// DefaultStarveInterval is the default maximum amount of time
	// that a Read will wait between receipts of stream data before
	// giving up and returning ErrIncomplete.
	DefaultStarveInterval = time.Minute
)

// ErrorString is an adapter that allows a string to serve as an error
type ErrorString string

// Error conforms to the stdlib error interface
func (es ErrorString) Error() string {
	return string(es)
}

const (
	// ErrKeyExists is returned when we are trying to write a key
	// that collides with existing data in Redis
	ErrKeyExists = ErrorString("redbytes: key already exists")

	// ErrIncomplete is returned to signify that we have returned
	// as much data as is available, but the write-side has not yet
	// indicated that the stream is finished.
	ErrIncomplete = ErrorString("redbytes: stream is incomplete")

	// ErrNoStimulus is returned from NewRedisByteStreamReader when
	// neither PollInterval(...) nor Subscribe(...) were configured.
	ErrNoStimulus = ErrorString("redbytes: no read stimulus defined")
)
