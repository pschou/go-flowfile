// Copyright 2023 pschou
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package flowfile provides a set of Functions to interact with NiFi FlowFiles
package flowfile

import (
	"hash"
	"io"
	"io/ioutil"
)

var (
	UserAgent   = "NiFi FlowFile Client (github.com/pschou/go-flowfile)"
	AboutString = "NiFi FlowFile Server (github.com/pschou/go-flowfile)"
	Debug       = false
)

// A File is a handler for either an incoming datafeed or outgoing datafeed
// of the contents of a file over a File connection.  The intent is for one
// to either provide a Reader to provide to a flowfile sender or read from the
// File directly as it implments the io.Reader interface.  Neither the
// reader or the counts are exported to avoid accidental over-reads of the
// underlying reader interface.
type File struct {
	Attrs Attributes
	i     int64 // file position
	n     int64 // bytes remaining
	Size  int64 // total size

	// one of the following must be set
	r  io.Reader   // underlying Read
	ra io.ReaderAt // underlying ReadAt

	cksumStatus int8
	cksum       hash.Hash
	closed      bool // bit to let the reciever know the up stream is really closed
}

// Create a new File struct from an io.Reader with size, one will want to add
// attributes before sending it off.
func New(r io.Reader, size int64) *File {
	f := &File{n: size, Size: size}
	if rs, ok := r.(io.ReadSeeker); ok {
		f.i, _ = rs.Seek(0, io.SeekCurrent)
	}
	if ra, ok := r.(io.ReaderAt); ok {
		f.ra = ra
	} else {
		f.r = r
	}
	return f
}

// Read will read the content from a FlowFile
func (l *File) Read(p []byte) (n int, err error) {
	if l.n <= 0 {
		return 0, io.EOF
	}
	if int64(len(p)) > l.n {
		p = p[0:l.n]
	}
	if l.ra != nil {
		n, err = l.ra.ReadAt(p, l.i)
	} else {
		n, err = l.r.Read(p)
	}
	if err == io.EOF {
		// Mark the closed bit
		l.closed = true
	}
	l.n -= int64(n)
	l.i += int64(n)
	if l.cksumStatus == cksumInit {
		l.cksum.Write(p[:n])
	}
	if err == nil && l.n <= 0 {
		err = io.EOF
	}
	return
}

// Close the flowfile.  Generally the FlowFile is acted upon in a streaming
// context, moving a file from one place to another.  So, in this
// understanding, the action of closing a file is effectively removing it from
// consideration and going to the next file.
func (l *File) Close() (err error) {
	if l.ra != nil {
		if rs, ok := l.ra.(io.ReadSeeker); ok {
			// Seek the pointer to the next reading position
			rs.Seek(l.n, io.SeekCurrent)
		}
	} else {
		_, err = io.CopyN(ioutil.Discard, l.r, l.n)
	}
	l.n, l.i = 0, l.i+l.n
	return
}

// Encode and write the FlowFile to an io.Writer
func (l *File) WriteTo(w io.Writer) error {
	return writeTo(w, l)
}
