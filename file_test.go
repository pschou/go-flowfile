package flowfile

import (
	"bytes"
	"fmt"
	"log"

	"github.com/pschou/go-flowfile"
)

// Sends files out a writer, making sure the headers are sent before each file is sent.
func ExampleSendFiles() {
	wire := bytes.NewBuffer([]byte{})
	{
		dat := []byte("this is a custom string for flowfile")
		ff := flowfile.New(bytes.NewReader(dat), int64(len(dat)))
		ff.Attrs.Set("path", "./")
		ff.Attrs.Set("filename", "abcd-efgh")
		flowfile.SendFiles(wire, []*flowfile.File{ff})
	}
	fmt.Printf("wire: %q\n", wire.String())

	// Output:
	// wire: "NiFiFF3\x00\x02\x00\x04path\x00\x02./\x00\bfilename\x00\tabcd-efgh\x00\x00\x00\x00\x00\x00\x00$this is a custom string for flowfile"
}

// This example shows how to write a FlowFile and then read in a stream to make a flowfile
func ExampleFlowFileReader() {
	wire := bytes.NewBuffer([]byte{})
	{
		dat := []byte("this is a custom string for flowfile")
		ff := flowfile.New(bytes.NewReader(dat), int64(len(dat)))
		ff.Attrs.Set("path", "./")
		ff.Attrs.Set("filename", "abcd-efgh")
		flowfile.SendFiles(wire, []*flowfile.File{ff})
	}

	r := flowfile.NewReader(wire)
	f, err := r.Read()
	if err == nil {
		fmt.Printf("attributes: %#v\n", f.Attrs)

		buf := bytes.NewBuffer([]byte{})
		buf.ReadFrom(f)
		fmt.Printf("content: %q\n", buf.String())
	} else {
		log.Println("Error reading ff:", err)
	}

	// Output:
	// attributes: flowfile.Attributes{flowfile.Attribute{Name:"path", Value:"./"}, flowfile.Attribute{Name:"filename", Value:"abcd-efgh"}}
	// content: "this is a custom string for flowfile"
}
