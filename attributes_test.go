package flowfile_test

import (
	"bytes"
	"fmt"
	"log"

	"github.com/pschou/go-flowfile"
)

// This show how to set an individual attribute
func ExampleAttributes_Set() {
	var a flowfile.Attributes
	fmt.Printf("attributes: %#v\n", a)

	a.Set("path", "./")
	fmt.Printf("attributes: %#v\n", a)
	// Output:
	// attributes: flowfile.Attributes(nil)
	// attributes: flowfile.Attributes{flowfile.Attribute{Name:"path", Value:"./"}}
}

// This show how to get an individual attribute
func ExampleAttributes_Get() {
	var a flowfile.Attributes
	a.Set("path", "./")

	fmt.Println("attribute:", a.Get("path"))
	// Output:
	// attribute: ./
}

// This show how to unset an individual attribute
func ExampleAttributes_Unset() {
	var a flowfile.Attributes
	a.Set("path", "./")
	a.Set("junk", "cars")
	a.Set("filename", "abcd-efgh")

	a.Unset("junk")
	fmt.Printf("attributes: %#v\n", a)
	// Output:
	// attributes: flowfile.Attributes{flowfile.Attribute{Name:"path", Value:"./"}, flowfile.Attribute{Name:"filename", Value:"abcd-efgh"}}
}

// This show how to encode the attributes into a header for sending
func ExampleAttributes_WriteTo() {
	var a flowfile.Attributes
	a.Set("path", "./")
	a.Set("filename", "abcd-efgh")

	buf := bytes.NewBuffer([]byte{})
	a.WriteTo(buf)

	fmt.Printf("attributes: %q\n", buf)
	// Output:
	// attributes: "NiFiFF3\x00\x02\x00\x04path\x00\x02./\x00\bfilename\x00\tabcd-efgh"
}

// This show how to decode the attributes frim a header for parsing
func ExampleAttributes_ReadFrom() {
	var a flowfile.Attributes
	wire := bytes.NewBuffer([]byte("NiFiFF3\x00\x02\x00\x04path\x00\x02./\x00\bfilename\x00\tabcd-efgh"))

	a.ReadFrom(wire)

	fmt.Printf("attributes: %#v\n", a)
	// Output:
	// attributes: flowfile.Attributes{flowfile.Attribute{Name:"path", Value:"./"}, flowfile.Attribute{Name:"filename", Value:"abcd-efgh"}}
}

// This show how to encode the attributes into a header for sending
func ExampleAttributes_Marshal() {
	var a flowfile.Attributes
	a.Set("path", "./")
	a.Set("filename", "abcd-efgh")

	fmt.Printf("attributes: %q\n", flowfile.MarshalAttributes(a))
	// Output:
	// attributes: "NiFiFF3\x00\x02\x00\x04path\x00\x02./\x00\bfilename\x00\tabcd-efgh"
}

// This show how to decode the attributes frim a header for parsing
func ExampleAttributes_Unmarshal() {
	var a flowfile.Attributes
	buf := []byte("NiFiFF3\x00\x02\x00\x04path\x00\x02./\x00\bfilename\x00\tabcd-efgh")

	err := flowfile.UnmarshalAttributes(buf, &a)
	if err != nil {
		log.Fatal("Error unmarshalling attributes:", err)
	}

	fmt.Printf("attributes: %#v\n", a)
	// Output:
	// attributes: flowfile.Attributes{flowfile.Attribute{Name:"path", Value:"./"}, flowfile.Attribute{Name:"filename", Value:"abcd-efgh"}}
}
