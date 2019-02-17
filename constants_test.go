package redbytes

import (
	"crypto/rand"
	"fmt"
	"io"
)

var alphabet = "abcdefghijklmnopqrstuvwxyz"

func randomString() string {
	b := make([]byte, 16)
	io.ReadFull(rand.Reader, b)
	return fmt.Sprintf("%x", b)
}
