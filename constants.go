package redbytes

const (
	chunkFmt = "chunk:%d"
	chunkEnd = "chunk:end"
	metadata = "metadata"

	// DefaultMaxChunkSize is the base defined width of each data field
	DefaultMaxChunkSize = 1024
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
)
