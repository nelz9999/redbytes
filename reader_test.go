package redbytes

import (
	"io"
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
