package flowfile_test

import (
	"bytes"
	"fmt"
	"log"
	"os"

	"github.com/pschou/go-flowfile"
)

// Sends files out a writer, making sure the headers are sent before each file is sent.
func ExampleNewEncoder() {
	wire := bytes.NewBuffer([]byte{})
	enc := flowfile.NewWriter(wire)
	{
		dat := []byte("this is a custom string for flowfile")
		ff := flowfile.New(bytes.NewReader(dat), int64(len(dat)))
		ff.Attrs.Set("path", "./")
		ff.Attrs.Set("filename", "abcd-efgh")
		enc.Write(ff)
	}
	fmt.Printf("wire: %q\n", wire.String())

	// Output:
	// wire: "NiFiFF3\x00\x02\x00\x04path\x00\x02./\x00\bfilename\x00\tabcd-efgh\x00\x00\x00\x00\x00\x00\x00$this is a custom string for flowfile"
}

// Sends files out a writer, making sure the headers are sent before each file is sent.
func ExampleUnmarshal() {
	dat := []byte("NiFiFF3\x00\x02\x00\x04path\x00\x02./\x00\bfilename\x00\tabcd-efgh\x00\x00\x00\x00\x00\x00\x00$this is a custom string for flowfile")

	var f flowfile.File
	err := flowfile.Unmarshal(dat, &f)
	if err != nil {
		fmt.Println("Error unmarshalling:", err)
	}
	fmt.Printf("Attrs: %v\n", f.Attrs)

	buf := bytes.NewBuffer([]byte{})
	buf.ReadFrom(&f)
	fmt.Printf("content: %q\n", buf.String())
	// Output:
	// Attrs: {"path":"./","filename":"abcd-efgh"}
	// content: "this is a custom string for flowfile"
}

// This example shows how to write a FlowFile and then read in a stream to make a flowfile
func ExampleNewScanner() {
	wire := bytes.NewBuffer([]byte("NiFiFF3\x00\x02\x00\x04path\x00\x02./\x00\bfilename\x00\tabcd-efgh\x00\x00\x00\x00\x00\x00\x00$this is a custom string for flowfile"))

	s := flowfile.NewScanner(wire)
	for s.Scan() { // Scan for another FlowFile in the stream
		f := s.File()

		fmt.Printf("attributes: %v\n", f.Attrs)

		buf := bytes.NewBuffer([]byte{})
		buf.ReadFrom(f)
		fmt.Printf("content: %q\n", buf.String())
	}
	fmt.Println("Check for errors:", s.Err())

	// Output:
	// attributes: {"path":"./","filename":"abcd-efgh"}
	// content: "this is a custom string for flowfile"
	// Check for errors: <nil>
}

// A calling method should do the due diligence of closing the inner reader
// after the flowfile is done being used.  A good way to do this is something
// like:
func ExampleNew() {
	dir, filename := "./", "myfile.dat"
	fh, err := os.Open(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer fh.Close() // Ensure the file is closed when the function exits
	fileInfo, _ := fh.Stat()
	f := flowfile.New(fh, fileInfo.Size()) // Construct a flowfile with size
	f.Attrs.Set("path", dir)               // Specify the path for the file
	f.Attrs.Set("filename", filename)      // Give the filename
	f.Attrs.GenerateUUID()                 // Set a unique identifier to this file

}
