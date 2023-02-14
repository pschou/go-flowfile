package flowfile // import "github.com/pschou/go-flowfile"

import (
	"bytes"
	"encoding/binary"
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
func (e *Writer) Write(f *File) (n int64, err error) {
	header := bytes.NewBuffer([]byte{})
	if err = f.Attrs.WriteTo(header); err != nil {
		if Debug {
			log.Println("Failed to send size header", err)
		}
		return
	}

	if err = binary.Write(header, binary.BigEndian, uint64(f.Size)); err != nil {
		if Debug {
			log.Println("Failed to send size header", err)
		}
		return
	}

	n, err = io.Copy(e.w, io.MultiReader(header, f))

	if Debug && err != nil {
		log.Println("Failed to send contents", err)
	}

	return
}

// Marshal a FlowFile into a byte slice.
//
// Note: This is not preferred as it can cause memory bloat.
func Marshal(f *File) (dat []byte, err error) {
	buf := bytes.NewBuffer(dat)
	enc := &Writer{w: buf}
	_, err = enc.Write(f)
	dat = buf.Bytes()
	return
}

// FromReader reads a FlowFile from an io.Reader, parses the attributes
// and returns a File struct for processing.
func ParseOne(in io.Reader) (f *File, err error) {
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
func Unmarshal(dat []byte, f *File) (err error) {
	var ff *File
	ff, err = ParseOne(bytes.NewReader(dat))
	if ff != nil {
		*f = *ff
	}
	return
}
