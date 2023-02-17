package flowfile

import (
	"bytes"
	"fmt"
	"io"
	"strings"
)

// Read the entire payload into a buffer, so as to complete the checksum and
// enable the ability to reset the File for multiple reads.
//
// Note: This could create memory bloat if the buffers are not able to be
// cleared out due to the runtime keeping an unused pointer or the buffer isn't
// returned to a Pool.
func (f *File) BufferFile(buf *bytes.Buffer) (err error) {
	if _, ok := f.ra.(*bytes.Reader); ok {
		// The payload is already in a byte reader
		return nil
	}
	if _, ok := f.ra.(*strings.Reader); ok {
		// The payload is already in a strings reader
		return nil
	}

	// Reset the file if we have an empty file or a ReadAt interface
	if f.Size == 0 {
		f.r, f.ra, f.filePath = nil, bytes.NewReader([]byte{}), ""
		return nil
	} else if f.ra != nil || f.filePath != "" {
		f.i, f.n = f.i+f.n-f.Size, f.Size
	} else if f.r != nil && f.n != f.Size {
		return fmt.Errorf("File already started being read, cannot unread bytes")
	}

	// Copy the whole thing to the buffer!
	buf.Reset()
	if _, err = io.Copy(buf, f); err != nil {
		return
	}

	// Reset the pointers for using the new buffer reader
	f.i, f.n, f.r, f.ra, f.filePath = 0, f.Size, nil, bytes.NewReader(buf.Bytes()), ""
	return
}
