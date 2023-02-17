package flowfile_test

import (
	"bytes"
	"fmt"
	"log"

	"github.com/pschou/go-flowfile"
)

func ExampleCustodyChainShift() {
	var a flowfile.Attributes
	a.Set("filename", "abcd-efgh")
	a.Set("custodyChain.0.host", "data")
	a.Set("custodyChain.10.time", "now")
	a.Set("custodyChain.6.time", "now")

	a.CustodyChainShift()
	a.Sort()

	a.Unset("custodyChain.0.time")
	a.Unset("custodyChain.0.local.hostname")
	fmt.Printf("attributes: %s\n", a.IntentedString())
	// Output:
	// attributes: {
	//   "custodyChain.1.host":"data",
	//   "custodyChain.7.time":"now",
	//   "custodyChain.11.time":"now",
	//   "filename":"abcd-efgh"
	// }
}

// This show how to set an individual attribute
func ExampleAttributes_Set() {
	var a flowfile.Attributes
	fmt.Printf("attributes: %v\n", a)

	a.Set("path", "./")
	fmt.Printf("attributes: %v\n", a)
	// Output:
	// attributes: {}
	// attributes: {"path":"./"}
}

// This show how to get an individual attribute
func ExampleAttributes_ByteLen() {
	var a flowfile.Attributes
	a.Set("path", "./")
	a.Set("val", "a")

	b := flowfile.MarshalAttributes(a)
	fmt.Println("attribute len:", flowfile.HeaderSize(&flowfile.File{Attrs: a}), len(b)+8)
	// Output:
	// attribute len: 35 35
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
	fmt.Printf("attributes: %v\n", a)
	// Output:
	// attributes: {"path":"./","filename":"abcd-efgh"}
}

// This show how to encode the attributes into a header for sending
func ExampleAttributes_WriteTo() {
	var a flowfile.Attributes
	a.Set("path", "./")
	a.Set("filename", "abcd-efgh")

	buf := bytes.NewBuffer([]byte{})
	a.WriteTo(buf)

	fmt.Printf("raw: %q\n", buf)
	// Output:
	// raw: "NiFiFF3\x00\x02\x00\x04path\x00\x02./\x00\bfilename\x00\tabcd-efgh"
}

// This show how to decode the attributes frim a header for parsing
func ExampleAttributes_ReadFrom() {
	var a flowfile.Attributes
	wire := bytes.NewBuffer([]byte("NiFiFF3\x00\x02\x00\x04path\x00\x02./\x00\bfilename\x00\tabcd-efgh"))

	a.ReadFrom(wire)

	fmt.Printf("attributes: %v\n", a)
	// Output:
	// attributes: {"path":"./","filename":"abcd-efgh"}
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

	fmt.Printf("attributes: %v\n", a)
	// Output:
	// attributes: {"path":"./","filename":"abcd-efgh"}
}
