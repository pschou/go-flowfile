package flowfile

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/djherbis/times"
)

// If the File was created with NewFromDisk, return the filename referenced.
func (f File) FilePath() string {
	return f.filePath
}

// NewFromDisk creates a new File struct from a file on disk.  One should add
// attributes before writing it to a stream.
//
// Note that the file is not opened to keep the opened file pointers on the
// system at a minimum.  However, once a file is used, the file handle remains
// open until Close() is called.  It is recommended that a checksum is done on
// the file before sending.
func NewFromDisk(filename string) (*File, error) {
	f := &File{filePath: filename}
	var err error
	f.fileInfo, err = os.Lstat(filename)
	if err != nil {
		return nil, err
	}

	dn, fn := path.Split(filename)
	if dn == "" {
		dn = "./"
	}
	f.Attrs.add("path", dn)
	f.Attrs.add("filename", fn)
	f.Attrs.add("file.lastModifiedTime", f.fileInfo.ModTime().Format(time.RFC3339))
	if ts, err := times.Stat(filename); err == nil && ts.HasBirthTime() {
		f.Attrs.add("file.creationTime", ts.BirthTime().Format(time.RFC3339))
	} else {
		f.Attrs.add("file.creationTime", f.fileInfo.ModTime().Format(time.RFC3339))
	}
	f.Attrs.GenerateUUID()

	switch mode := f.fileInfo.Mode(); {
	case mode.IsRegular():
		f.Size = f.fileInfo.Size()
		f.n = f.Size
		f.Attrs.add("file.permissions", mode.String())
	case mode.IsDir():
		f.Attrs.add("kind", "dir")
	case mode&fs.ModeSymlink != 0:
		target, _ := os.Readlink(filename)

		if strings.HasPrefix(target, "/") {
			// Try to build a relative link instead of absolute path link so the link
			// doesn't break on transfer.
			cur := dn
			if !strings.HasPrefix(cur, "/") {
				wd, _ := os.Getwd()
				cur = path.Join(wd, cur)
			}
			if rel, err := filepath.Rel(cur, target); err == nil &&
				!strings.HasPrefix(rel, "..") {
				target = rel
			}
		}
		f.Attrs.add("kind", "link")
		f.Attrs.add("target", target)
	default:
		return nil, fmt.Errorf("Invalid file: %q", filename)
	}
	return f, nil
}
