package flowfile // import "github.com/pschou/go-flowfile"

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
)

// Encode a flowfile into an io.Writer
func writeTo(out io.Writer, f *File) (err error) {
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

	_, err = io.Copy(out, io.MultiReader(header, f))
	if Debug && err != nil {
		log.Println("Failed to send contents", err)
	}

	return
}

// Marshal a FlowFile into a byte slice.
//
// Note: This is not preferred as it can cause memory bloat.
func Marshal(f File) (dat []byte, err error) {
	buf := bytes.NewBuffer(dat)
	err = f.WriteTo(buf)
	dat = buf.Bytes()
	return
}

// Parse an io.Reader of raw FlowFile formatted byte stream into a File struct
// for processing.
func Parse(in io.Reader) (f *File, err error) {
	var a Attributes
	if err = a.ReadFrom(in); err != nil {
		return
	}
	var N uint64
	if err = binary.Read(in, binary.BigEndian, &N); err != nil {
		err = fmt.Errorf("Error parsing file size: %s", err)
		return
	}
	f = &File{Size: int64(N), n: int64(N), Attrs: a}

	if rs, ok := in.(io.ReadSeeker); ok {
		f.i, _ = rs.Seek(0, io.SeekCurrent)
	}
	if ra, ok := in.(io.ReaderAt); ok {
		f.ra = ra
	} else {
		f.r = in
	}
	return
}

// Parse FlowFile formatted byte slice into a File struct for processing.
//
// Note: This is not preferred as it can cause memory bloat.
func Unmarshal(dat []byte) (f *File, err error) {
	f = new(File)
	return Parse(bytes.NewBuffer(dat))
}
