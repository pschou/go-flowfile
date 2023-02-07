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

// GoLang module flowfile provides a set of tools to interact with NiFi
// FlowFiles at a low level.  It's been finely tuned to handle the streaming
// context best as memory and disk often have limitations.
//
// This module was built to be both simple to use and extremely low level set
// of tools to work with FlowFiles at wire speed.  Here is an example of a
// basic filtering and forwarding method:
//
//   // Create a endpoint to send FlowFiles to:
//   txn, err := flowfile.NewHTTPTransaction("http://localhost:8080/contentListener", http.DefaultClient)
//   if err != nil {
//     log.Fatal(err)
//   }
//
//   // Setup a receiver method to deal with incoming flowfiles
//   myFilter := flowfile.NewHTTPFileReceiver(func(f *flowfile.File, r *http.Request) error {
//     // Logic here starts at the first packet in the stream, by the time a decision is
//     // made the streams are able to be connected together to avoid all local caches.
//     if f.Attrs.Get("project") == "ProjectA" {
//       return txn.Send(f)  // Forward only ProjectA related FlowFiles
//     }
//     return nil            // Drop the rest
//   })
//
//   http.Handle("/contentListener", myFilter)  // Add the listener to a path
//   http.ListenAndServe(":8080", nil)          // Start accepting connections
//
//
// The complexity of the decision logic can be as complex or as simple as one
// desires and consume on one or more ports / listening paths, and send to as
// many upstream servers as desired with concurrency.
//
// # About FlowFiles
//
// FlowFiles are at the heart of Apache NiFi and its flow-based design. A
// FlowFile is a data record, which consists of a pointer to its content
// (payload) and attributes to support the content, that is associated with one
// or more provenance events. The attributes are key/value pairs that act as
// the metadata for the FlowFile, such as the FlowFile filename. The content is
// the actual data or the payload of the file.
//
// # Implementations
//
// The `File` entity represents the flowfile.  The FlowFile Attributes can be
// inspected on the wire, before the payload is consumed.  This way one can
// craft switching logic and apply rules before the payload has been brougn in.
//
// One can compare this design, of acting on the header alone, to that of a
// network switch/router in which the Layer 2/3 packet header is inspected
// before the entire packet is consumed to make a determination on which path
// the bytes should flow.  By this design, of not having, to have the entire
// file before logic can be done, pipes can be connected and memory use can be
// kept to a minimum.
//
// Standard usage has shown that the overall memory footprint of several
// utilities doing complex routing of FlowFiles remains around 10-20MB,
// conversely a standard NiFi package will take a minimum of approximately
// 800MB and up.
//
//
package flowfile // import "github.com/pschou/go-flowfile"

import (
	"fmt"
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
	openCount   *int
}

// Create a new File struct from an io.Reader with size.  One should add
// attributes before writing it to a stream.
func New(r io.Reader, size int64) *File {
	ct := 1
	f := &File{n: size, Size: size, openCount: &ct}
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

// If the flowfile has a ReaderAt interface, one can reset the
// reader to the start for reading again
func (l *File) Reset() error {
	if l.ra != nil {
		l.i, l.n = l.i-(l.Size-l.n), l.Size
		return nil
	}
	return fmt.Errorf("Unable to Reset a non-ReadAt reader")
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
	switch {
	case l.ra != nil:
		if rc, ok := l.ra.(io.Closer); ok && l.openCount != nil {
			if *l.openCount == 1 {
				rc.Close()
			}
			*l.openCount = *l.openCount - 1
		}
		// else if rs, ok := l.ra.(io.ReadSeeker); ok {
		// Seek the pointer to the next reading position
		//	rs.Seek(l.n, io.SeekCurrent)
		// }

	case l.r != nil:
		_, err = io.CopyN(ioutil.Discard, l.r, l.n)
		if rc, ok := l.r.(io.Closer); ok && l.openCount != nil {
			if *l.openCount == 1 {
				rc.Close()
			}
			*l.openCount = *l.openCount - 1
		}
	}
	// Adjust the counters
	l.n, l.i = 0, l.i+l.n
	return
}

// Encode and write the FlowFile to an io.Writer
func (l *File) WriteTo(w io.Writer) error {
	return writeTo(w, l)
}
