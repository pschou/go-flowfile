package flowfile_test

import (
	"bytes"
	"fmt"
	"log"

	"github.com/pschou/go-flowfile"
)

// Sends files out a writer, making sure the headers are sent before each file is sent.
func ExampleFile_WriteTo() {
	wire := bytes.NewBuffer([]byte{})
	{
		dat := []byte("this is a custom string for flowfile")
		ff := flowfile.New(bytes.NewReader(dat), int64(len(dat)))
		ff.Attrs.Set("path", "./")
		ff.Attrs.Set("filename", "abcd-efgh")
		ff.WriteTo(wire)
	}
	fmt.Printf("wire: %q\n", wire.String())

	// Output:
	// wire: "NiFiFF3\x00\x02\x00\x04path\x00\x02./\x00\bfilename\x00\tabcd-efgh\x00\x00\x00\x00\x00\x00\x00$this is a custom string for flowfile"
}

// Sends files out a writer, making sure the headers are sent before each file is sent.
func ExampleUnmarshal() {
	dat := []byte("NiFiFF3\x00\x02\x00\x04path\x00\x02./\x00\bfilename\x00\tabcd-efgh\x00\x00\x00\x00\x00\x00\x00$this is a custom string for flowfile")

	f, _ := flowfile.Unmarshal(dat)
	fmt.Printf("Attrs: %#v\n", f.Attrs)

	buf := bytes.NewBuffer([]byte{})
	buf.ReadFrom(f)
	fmt.Printf("content: %q\n", buf.String())
	// Output:
	// Attrs: flowfile.Attributes{flowfile.Attribute{Name:"path", Value:"./"}, flowfile.Attribute{Name:"filename", Value:"abcd-efgh"}}
	// content: "this is a custom string for flowfile"
}

// This example shows how to write a FlowFile and then read in a stream to make a flowfile
func ExampleNewScanner() {
	wire := bytes.NewBuffer([]byte("NiFiFF3\x00\x02\x00\x04path\x00\x02./\x00\bfilename\x00\tabcd-efgh\x00\x00\x00\x00\x00\x00\x00$this is a custom string for flowfile"))

	s := flowfile.NewScanner(wire)
	for s.Scan() { // Scan for another FlowFile in the stream
		f, err := s.File()
		if err != nil {
			log.Fatal("Error parsing ff:", err)
		}

		fmt.Printf("attributes: %#v\n", f.Attrs)

		buf := bytes.NewBuffer([]byte{})
		buf.ReadFrom(f)
		fmt.Printf("content: %q\n", buf.String())
	}

	// Output:
	// attributes: flowfile.Attributes{flowfile.Attribute{Name:"path", Value:"./"}, flowfile.Attribute{Name:"filename", Value:"abcd-efgh"}}
	// content: "this is a custom string for flowfile"
}
