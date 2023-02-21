package flowfile // import "github.com/pschou/go-flowfile"

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
)

type Writer struct {
	w io.Writer
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// Encode a flowfile into an io.Writer
func (f *File) EncodedReader() (rdr io.Reader) {
	header := bytes.NewBuffer([]byte{})
	f.Attrs.WriteTo(header)
	binary.Write(header, binary.BigEndian, uint64(f.Size))
	return io.MultiReader(header, f)
}

// Encode a flowfile into an io.Writer
func (e *Writer) Write(f *File) (n int64, err error) {
	n, err = io.Copy(e.w, f.EncodedReader())
	if Debug && err != nil {
		log.Println("Failed to send contents", err)
	}
	return
}

// Marshal a FlowFile into a byte slice.
//
// Note: This is not preferred as it can cause memory bloat.
func (f *File) MarshalBinary(dat []byte, err error) {
	buf := bytes.NewBuffer(dat)
	enc := &Writer{w: buf}
	_, err = enc.Write(f)
	dat = buf.Bytes()
	return
}

// parseOne reads a FlowFile from an io.Reader, parses the attributes
// and returns a File struct for processing.
func parseOne(in io.Reader) (f *File, err error) {
	var a Attributes
	if err = a.ReadFrom(in); err != nil {
		return
	}
	var N uint64
	if err = binary.Read(in, binary.BigEndian, &N); err != nil {
		return nil, fmt.Errorf("Error parsing file size: %s", err)
	}

	f = &File{Size: int64(N), n: int64(N), Attrs: a}

	if ra, ok := in.(io.ReaderAt); ok {
		if rs, ok := in.(io.ReadSeeker); ok {
			// If a read seeker is implemented, get our current position so we can
			// make sure we stay in the right place or enable resetting.
			f.i, _ = rs.Seek(0, io.SeekCurrent)
		}
		f.ra = ra
	} else {
		f.r = in
	}
	return
}

// Unmarshal parses a FlowFile formatted byte slice into a File struct for
// processing.
//
// Note: This is not preferred as it can cause memory bloat.
func (f *File) UnmarshalBinary(dat []byte) (err error) {
	var ff *File
	ff, err = parseOne(bytes.NewReader(dat))
	if err == nil {
		if int64(ff.HeaderSize())+ff.Size != int64(len(dat)) {
			return ErrorInconsistantSize
		}
		*f = *ff
	}
	return
}

var ErrorInconsistantSize = errors.New("Inconsistant flowfile size")
