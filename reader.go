package flowfile

import (
	"encoding/binary"
	"fmt"
	"io"
)

// A wrapper around an io.Reader which parses out the flow files.
type Reader struct {
	r    io.Reader
	err  error
	last *File
}

// Create a new FlowFile reader, wrapping io.Reader
func NewReader(in io.Reader) *Reader {
	return &Reader{
		r: in,
	}
}

// Get the next FlowFile off the input
func (r *Reader) Read() (f *File, err error) {
	if r.last != nil {
		r.last.Close()
		r.last = nil
	}
	if r.err != nil {
		return nil, r.err
	}
	// Loop over files as long as there is more data available:
	var a Attributes
	if err = a.Unmarshall(r.r); err != nil {
		return
	}
	var N uint64
	if err = binary.Read(r.r, binary.BigEndian, &N); err != nil {
		err = fmt.Errorf("Error parsing file size: %s", err)
		return
	}
	r.last = New(r.r, int64(N))
	r.last.Attrs = a
	return r.last, nil
}
