package flowfile

import (
	"bytes"
	"fmt"
)

// This show how to set an individual attribute
func ExampleAttributesSet() {
	a := Attributes{}
	fmt.Printf("attributes: %#v\n", a)

	a.Set("path", "./")
	fmt.Printf("attributes: %#v\n", a)
	// Output:
	// attributes: flowfile.Attributes{}
	// attributes: flowfile.Attributes{flowfile.Attribute{Name:"path", Value:"./"}}
}

// This show how to get an individual attribute
func ExampleAttributesGet() {
	var a Attributes
	a.Set("path", "./")

	fmt.Println("attribute:", a.Get("path"))
	// Output:
	// attribute: ./
}

// This show how to unset an individual attribute
func ExampleAttributesUnset() {
	var a Attributes
	a.Set("path", "./")
	a.Set("junk", "cars")
	a.Set("filename", "abcd-efgh")

	a.Unset("junk")
	fmt.Printf("attributes: %#v\n", a)
	// Output:
	// attributes: flowfile.Attributes{flowfile.Attribute{Name:"path", Value:"./"}, flowfile.Attribute{Name:"filename", Value:"abcd-efgh"}}
}

// This show how to encode the attributes into a header for sending
func ExampleAttributesMarshal() {
	var a Attributes
	a.Set("path", "./")
	a.Set("filename", "abcd-efgh")

	buf := bytes.NewBuffer([]byte{})
	a.WriteTo(buf)

	fmt.Printf("attributes: %q\n", buf)
	// Output:
	// attributes: "NiFiFF3\x00\x02\x00\x04path\x00\x02./\x00\bfilename\x00\tabcd-efgh"
}

// This show how to decode the attributes frim a header for parsing
func ExampleAttributesUnmarshal() {
	var a, b Attributes
	a.Set("path", "./")
	a.Set("filename", "abcd-efgh")

	buf := bytes.NewBuffer([]byte{})
	a.WriteTo(buf)
	b.ReadFrom(buf)

	fmt.Printf("attributes: %#v\n", b)
	// Output:
	// attributes: flowfile.Attributes{flowfile.Attribute{Name:"path", Value:"./"}, flowfile.Attribute{Name:"filename", Value:"abcd-efgh"}}
}
