package flowfile // import "github.com/pschou/go-flowfile"

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"errors"
	"fmt"
	"hash"
	"io"
	"log"
	"os"
	"strings"
	"sync"
)

const (
	cksumPreinit = iota
	cksumInit
	cksumFailed
	cksumPassed
	cksumUnverified
)

var (
	ErrorChecksumMismatch = errors.New("Mismatching checksum")
	ErrorChecksumMissing  = errors.New("Missing checksum")
	ErrorChecksumNoInit   = errors.New("Checksum was not initialized")
)

// Verify the file sent was complete and accurate
func (l *File) Verify() error {
	//if Debug {
	//	log.Println("Verify called, with checksum status =", l.cksumStatus, l.n)
	//}
	if l.Size == 0 && l.n == 0 {
		return nil
	}
	switch l.cksumStatus {
	case cksumInit:
		hashval := l.cksum.Sum(nil)
		if fmt.Sprintf("%0x", hashval) == l.Attrs.Get("checksum") {
			l.cksumStatus = cksumPassed
			return nil
		}
		l.cksumStatus = cksumFailed
		if Debug {
			log.Println("checksum:", fmt.Sprintf("%0x", hashval), "!= attr:", l.Attrs.Get("checksum"))
		}
		return ErrorChecksumMismatch
	case cksumPassed:
		return nil
	case cksumFailed:
		return ErrorChecksumMismatch
	}
	return ErrorChecksumMissing
}

// AddChecksumFromVerify will take the checksum computed in the verify step and set the checksum attribute to match.  This effectively makes a FlowFile pass what may other be a failed verification.  Useful for updating a checksum to an existing flowfile after it has been fully read in.
func (l *File) AddChecksumFromVerify() error {
	if l.Size == 0 && l.n == 0 {
		return nil
	}
	if l.cksumStatus == cksumInit {
		hashval := l.cksum.Sum(nil)
		l.Attrs.Set("checksum", fmt.Sprintf("%0x", hashval))
		return nil
	}
	return ErrorChecksumNoInit
}

// Verify a given hash against the file sent, to ensure a complete and accurate
// payload.
func (l *File) VerifyHash(h hash.Hash) error {
	if h == nil {
		return errors.New("Invalid hash")
	}
	hashval := h.Sum(nil)
	if fmt.Sprintf("%0x", hashval) == l.Attrs.Get("checksum") {
		return nil
	}
	return fmt.Errorf("checksum: %0x != attr: %s", hashval,
		l.Attrs.Get("checksum"))
}

// VerifyDetails describes why a match was successful or failed
func (l *File) VerifyDetails() string {
	switch l.cksumStatus {
	case cksumPassed:
		hashval := l.cksum.Sum(nil)
		return fmt.Sprintf("Checksum values matched %q = %q (%d of %d bytes)", fmt.Sprintf("%0x", hashval), l.Attrs.Get("checksum"), l.n, l.Size)
	case cksumFailed:
		hashval := l.cksum.Sum(nil)
		return fmt.Sprintf("Checksum values differ %q != %q (%d of %d bytes)", fmt.Sprintf("%0x", hashval), l.Attrs.Get("checksum"), l.n, l.Size)
	}
	return fmt.Sprintf("No details available for checksum result")
}

// Verify the file sent was complete and accurate
func (l *File) VerifyParent(fp string) error {
	if ct := l.Attrs.Get("segment.original.checksumType"); ct != "" {
		new := getChecksumFunc(ct)
		if new == nil {
			return fmt.Errorf("Invalid original checksumType")
		}
		cksum := new()
		if fh, err := os.Open(fp); err != nil {
			return err
		} else {
			io.Copy(cksum, fh)
			fh.Close()
		}

		p_ck := l.Attrs.Get("segment.original.checksum")
		ck := fmt.Sprintf("%0x", cksum.Sum(nil))
		if p_ck != ck {
			return fmt.Errorf("Original checksum mismatch %q != %q", p_ck, ck)
		}

		// All is well now!
		return nil
	}
	return fmt.Errorf("No segment.original.checksumType")
}

// Create a new checksum for verifying payload.
func (h Attributes) NewChecksumHash() hash.Hash {
	if ct := h.Get("checksumType"); ct != "" {
		new := getChecksumFunc(ct)
		if new != nil {
			return new()
		}
	}
	return nil
}

// Function called before a file is read for setting up the hashing function.
func (l *File) ChecksumInit() error {
	if Debug {
		log.Println("Checksum init for", l.Attrs.Get("filename"))
	}
	if l.Size > 0 {
		if ct := l.Attrs.Get("checksumType"); ct != "" {
			new := getChecksumFunc(ct)
			if new != nil {
				l.cksum = new()
				l.cksumStatus = cksumInit
				return nil
			}
		}
		l.cksumStatus = cksumUnverified
		return errors.New("Unable to find matching checksum type")
	}
	return nil
}

// Add checksum to flowfile, requires a ReadAt interface in the flowfile context.
//
// Note: The checksums cannot be added to a streamed File (io.Reader) as the
// header would have already been sent and could not be placed in the header as
// the payload would have been sent on the wire already.  Hence, read the
// content, build checksum and add to header.   Hence why the io.ReaderAt
// interface is important.
func (f *File) AddChecksum(cksum string) error {
	if f.Size == 0 {
		return nil // Don't add checksum for empty files
	}
	new := getChecksumFunc(cksum)
	if new == nil {
		return fmt.Errorf("Unable to find checksum type: %q", cksum)
	}

	var ra io.ReaderAt
	if f.ra != nil {
		ra = f.ra
	}

	// Case where the file is not currently open, open and do the checksum and close
	if ra == nil && f.filePath != "" {
		if Debug {
			log.Println("Opening file for checksum", f.filePath)
		}
		if fh, err := os.Open(f.filePath); err != nil {
			return err
		} else {
			ra = fh
			defer func() {
				if Debug {
					log.Println("Closing file after checksum", f.filePath)
				}
				fh.Close()
			}()
		}
	}

	if ra != nil {
		// We have a ReadAt reader, do the checksum!
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
					f.Attrs.Set("checksumType", cksum)
					f.Attrs.Set("checksum", fmt.Sprintf("%0x", h.Sum(nil)))
					return nil
				}
			}
			if err != nil {
				if err == io.EOF {
					return nil
				} else {
					if Debug {
						log.Println("Reading for checksum ran into error", err)
					}
					return err
				}
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
