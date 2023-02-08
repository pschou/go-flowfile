package flowfile // import "github.com/pschou/go-flowfile"

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Save will save the flowfile to a given directory, reconstructing the
// original directory tree with files in it while doing checksums on each file
// as they are layed down.  It is up to the calling function to determine
// whether to delete or keep the file after an unsuccessful send.
func (f *File) Save(baseDir string) (outputFile string, err error) {
	dir := filepath.Clean(f.Attrs.Get("path"))
	if strings.HasPrefix(dir, "..") {
		err = fmt.Errorf("Invalid path %q", dir)
		return
	}
	dir = path.Join(baseDir, dir)
	if _, err = os.Stat(dir); os.IsNotExist(err) {
		if err = os.MkdirAll(dir, 0755); err != nil {
			return
		}
	}

	_, filename := path.Split(f.Attrs.Get("filename"))
	outputFile = path.Join(dir, filename)
	var fh *os.File

	if sz := f.Attrs.Get("segment.original.size"); sz == "" {
		// Open a file for whole writeout, write the file, then checksum
		if fh, err = os.Create(outputFile); err != nil {
			return
		}
		defer fh.Close() // Make sure file is closed at the end of the function

		// Write out file contents
		if _, err = io.Copy(fh, f); err != nil {
			return
		}
		if f.Size > 0 {
			err = f.Verify() // Return the verification of the checksum
		}
	} else {
		var parentSize, offset uint64
		if parentSize, err = strconv.ParseUint(sz, 10, 64); err != nil {
			return
		}
		if offset, err = strconv.ParseUint(f.Attrs.Get("fragment.offset"), 10, 64); err != nil {
			return
		}
		// Make sure the target file is in place and has the right size:
		fh, err = os.OpenFile(outputFile, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0666)
		if err == nil {
			io.Copy(fh, &zeros{n: parentSize})
			fh.Close()
		}

		var stat os.FileInfo
		stat, err = os.Stat(outputFile)
		for i := 0; err != nil && i < 10 || uint64(stat.Size()) < parentSize; i++ {
			time.Sleep(3 * time.Second)
			stat, err = os.Stat(outputFile)
		}
		if uint64(stat.Size()) == parentSize {
			if fh, err = os.OpenFile(outputFile, os.O_RDWR, 0600); err != nil {
				return
			}
			defer fh.Close() // Make sure file is closed at the end of the function

			var newOffset int64
			if newOffset, err = fh.Seek(int64(offset), io.SeekStart); err != nil {
				return
			} else if uint64(newOffset) != offset {
				err = fmt.Errorf("Not able to seek to correct offset %d != %d", newOffset, offset)
				return
			}

			// Write out the segment contents
			if _, err = io.Copy(fh, f); err != nil {
				return
			}
		}
	}
	return
}

type zeros struct {
	n uint64
}

func (z *zeros) Read(p []byte) (n int, err error) {
	if uint64(len(p)) < uint64(z.n) {
		z.n, n = z.n-uint64(len(p)), len(p)
	} else {
		n, z.n = int(z.n), 0
		err = io.EOF
	}
	return
}
