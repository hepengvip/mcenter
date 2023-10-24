package mcenterproto

import (
	"bufio"
	"io"
)

func ReadFull(r *bufio.Reader, buff []byte) error {
	_, err := io.ReadFull(r, buff)
	return err
}

func ReadHeader(r *bufio.Reader, sep byte) (*[]byte, error) {
	data, err := r.ReadBytes(sep)
	return &data, err
}
