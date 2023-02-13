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

// Go-flowfile is a light weight connection handling tool for
// sending and receiving FlowFiles via an HTTP/HTTPS exchange.  When the HTTPS
// method is used, the client MUST also present a valid client certificate.
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
// More info: https://nifi.apache.org/docs/nifi-docs/html/nifi-in-depth.html
//
// # What is a FlowFile
//
// A FlowFile is a logical notion that correlates a piece of data with a set of
// Attributes about that data. Such attributes include a FlowFile's unique
// identifier, as well as its name, size, and any number of other flow-specific
// values. While the contents and attributes of a FlowFile can change, the
// FlowFile object is immutable. Modifications to a FlowFile are made possible
// by the ProcessSession.
//
// The core attributes for FlowFiles are defined in the
// org.apache.nifi.flowfile.attributes.CoreAttributes enum. The most common
// attributes you'll see are filename, path and uuid. The string in quotes is
// the value of the attribute within the CoreAttributes enum.
//
// - Filename ("filename"): The filename of the FlowFile. The filename should
// not contain any directory structure.
//
// - UUID ("uuid"): A unique universally unique identifier (UUID) assigned to
// this FlowFile.
//
// - Path ("path"): The FlowFile's path indicates the relative directory to
// which a FlowFile belongs and does not contain the filename.
//
// - Absolute Path ("absolute.path"): The FlowFile's absolute path indicates
// the absolute directory to which a FlowFile belongs and does not contain the
// filename.
//
// - Priority ("priority"): A numeric value indicating the FlowFile priority.
//
// - MIME Type ("mime.type"): The MIME Type of this FlowFile.
//
// - Discard Reason ("discard.reason"): Specifies the reason that a FlowFile is
// being discarded.
//
// - Alternative Identifier ("alternate.identifier"): Indicates an identifier
// other than the FlowFile's UUID that is known to refer to this FlowFile.
//
// More info: https://docs.cloudera.com/HDPDocuments/HDF3/HDF-3.0.3/bk_developer-guide/content/flowfile.html
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
	"os"
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
	ra io.ReaderAt // underlying ReadAt (if available)

	// If a ReadFile is called
	filePath string      // path to file on disk
	fileInfo os.FileInfo // information about the file

	// Checksum holder for post-stream checksum verification
	cksumStatus int8
	cksum       hash.Hash
}

// Create a new File struct from an io.Reader with size.  One should add
// attributes before writing it to a stream.
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

// If the flowfile has a ReaderAt interface, one can reset the
// reader to the start for reading again
func (l *File) Reset() error {
	if l.Size == 0 {
		return nil
	} else if l.ra != nil || l.filePath != "" {
		l.i, l.n = l.i+l.n-l.Size, l.Size
		return nil
	}
	if Debug {
		fmt.Printf("Reset called on %#v\n", l)
	}
	return fmt.Errorf("Unable to Reset a non-ReadAt reader")
}

// Read will read the content from a FlowFile
func (l *File) Read(p []byte) (n int, err error) {
	if l.n <= 0 || l.Size == 0 {
		return 0, io.EOF
	}
	if l.filePath != "" && l.ra == nil {
		fh, err := os.Open(l.filePath)
		if err != nil {
			return 0, err
		}
		l.ra = fh
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

// Close the flowfile contruct.  Generally the FlowFile is acted upon in a
// streaming context, moving a file from one place to another.  So, in this
// understanding, the action of closing a flowfile is effectively removing the
// current payload from consideration and moving the reader pointer forward,
// making the next flowfile available for reading.
func (l *File) Close() (err error) {
	if l.filePath != "" {
		fh := l.ra.(*os.File)
		return fh.Close()
	}

	switch {
	case l.ra != nil:
	case l.r != nil:
		if rs, ok := l.ra.(io.ReadSeeker); ok {
			// Seek the pointer to the next reading position
			rs.Seek(l.n, io.SeekCurrent)
		} else {
			_, err = io.CopyN(ioutil.Discard, l.r, l.n)
		}
	default:
		return fmt.Errorf("Missing underlying reader")
	}
	// Adjust the counters
	l.n, l.i = 0, l.i+l.n
	return
}

// Encode and write the FlowFile to an io.Writer
//func (l *File) Encode(w io.Writer) (int64, error) {
//	return writeTo(w, l)
//}
