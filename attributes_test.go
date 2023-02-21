package flowfile_test

import (
	"bytes"
	"encoding/json"
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

	var out bytes.Buffer
	json.Indent(&out, []byte(a.String()), "", "  ")
	fmt.Printf("attributes: %s\n", out.String())
	// Output:
	// attributes: {
	//   "custodyChain.1.host": "data",
	//   "custodyChain.7.time": "now",
	//   "custodyChain.11.time": "now",
	//   "filename": "abcd-efgh"
	// }
}

func ExampleAttributes_UnmarshalJson() {
	var a flowfile.Attributes
	err := a.UnmarshalJSON([]byte(`{
    "custodyChain.1.host":"data",
    "custodyChain.7.time":"now",
    "custodyChain.11.time":"now",
    "filename":"abcd-efgh"
  }`))
	if err != nil {
		log.Println("error:", err)
	}
	fmt.Println("attrs:", a.String())
	// Output:
	// attrs: {"custodyChain.1.host":"data","custodyChain.7.time":"now","custodyChain.11.time":"now","filename":"abcd-efgh"}
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
func ExampleHeaderSize() {
	var a flowfile.Attributes
	a.Set("path", "./")
	a.Set("val", "a")

	b, _ := a.MarshalBinary()
	f := flowfile.New(bytes.NewReader([]byte{}), 0)
	f.Attrs = a
	fmt.Println("attribute len:", f.HeaderSize(), len(b)+8)
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
func ExampleAttributes_MarshalBinary() {
	var a flowfile.Attributes
	a.Set("path", "./")
	a.Set("filename", "abcd-efgh")

	b, _ := a.MarshalBinary()
	fmt.Printf("attributes: %q\n", b)
	// Output:
	// attributes: "NiFiFF3\x00\x02\x00\x04path\x00\x02./\x00\bfilename\x00\tabcd-efgh"
}

// This show how to decode the attributes frim a header for parsing
func ExampleAttributes_UnmarshalBinary() {
	var a flowfile.Attributes
	buf := []byte("NiFiFF3\x00\x02\x00\x04path\x00\x02./\x00\bfilename\x00\tabcd-efgh")

	err := a.UnmarshalBinary(buf)
	if err != nil {
		log.Fatal("Error unmarshalling attributes:", err)
	}

	fmt.Printf("attributes: %v\n", a)
	// Output:
	// attributes: {"path":"./","filename":"abcd-efgh"}
}
