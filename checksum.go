package flowfile

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"errors"
	"fmt"
	"hash"
	"strings"
	"sync"
)

const (
	cksumPreinit = iota
	cksumInit
	cksumFailed
	cksumPassed
)

var (
	ErrorChecksumMismatch = errors.New("Mismatching checksum")
	ErrorChecksumMissing  = errors.New("Missing checksum")
)

// Verify the file sent was complete and accurate
func (l *File) Verify() error {
	switch l.cksumStatus {
	case cksumInit:
		hashval := l.cksum.Sum(nil)
		if fmt.Sprintf("%0x", hashval) == l.Attrs.Get("checksum") {
			l.cksumStatus = cksumPassed
			return nil
		}
		l.cksumStatus = cksumFailed
		return ErrorChecksumMismatch
	case cksumPassed:
		return nil
	case cksumFailed:
		return ErrorChecksumMismatch
	}
	return ErrorChecksumMissing
}

// Internal function called before a file is read for setting up the hashing function.
func (l *File) cksumInit() {
	new := getChecksumFunc(l.Attrs.Get("checksum-type"))
	if new != nil {
		l.cksum = new()
		l.cksumStatus = cksumInit
	}
}

// Add checksum to flowfile, requires a ReadAt interface in the flowfile context.
//
// Note: The checksums cannot be added to a stream as the header would have already
// been sent, hence why the ReadAt interface is important.
func (f *File) AddChecksum(cksum string) error {
	new := getChecksumFunc(cksum)
	if new == nil {
		return fmt.Errorf("Unable to find checksum type: %q", cksum)
	}
	if ra := f.ra; ra != nil {
		bufp := bufPool.Get().(*[]byte)
		defer bufPool.Put(bufp)
		buf := *bufp
		h := new()
		n := f.n
		i := f.i

		for {
			if int64(len(buf)) > n {
				buf = buf[:n]
			}
			nr, err := ra.ReadAt(buf, i)
			if nr > 0 {
				h.Write(buf[0:nr])
				i += int64(nr)
				n -= int64(nr)
				if n == 0 {
					f.Attrs.Set("checksum-type", cksum)
					f.Attrs.Set("checksum", fmt.Sprintf("%0x", h.Sum(nil)))
					return nil
				}
			}
			if err != nil {
				return err
			}
		}
	}
	return fmt.Errorf("Reader must implement a ReadAt interface")
}

// Hash builder function
func getChecksumFunc(cksum string) func() hash.Hash {
	switch strings.TrimSpace(strings.ToUpper(cksum)) {
	case "MD5":
		return md5.New
	case "SHA1", "SHA":
		return sha1.New
	case "SHA224":
		return sha256.New224
	case "SHA256":
		return sha256.New
	case "SHA384":
		return sha512.New384
	case "SHA512":
		return sha512.New
	}
	return nil
}

var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 32*1024)
		return &b
	},
}
