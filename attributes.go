package flowfile

import (
	"encoding/binary"
	"fmt"
	"io"
	"net/http"

	"github.com/google/uuid"
)

// A single attribue in a FlowFile header
type Attribute struct {
	Name, Value string
}

// A set of attributes in a FlowFile header
type Attributes []Attribute

// Clone the attributes for ease of duplication
func (h Attributes) Clone() Attributes {
	attrs := []Attribute(h)
	out := make([]Attribute, len(attrs))
	for i := range attrs {
		out[i].Name = attrs[i].Name
		out[i].Value = attrs[i].Value
	}
	return Attributes(out)
}

// Returns the first attribute's value with specified name
func (h *Attributes) Get(name string) string {
	for _, elm := range []Attribute(*h) {
		if elm.Name == name {
			return elm.Value
		}
	}
	return ""
}

// Set a new UUID value for a FlowFile
func (h *Attributes) GenerateUUID() string {
	puuid := uuid.New().String()
	h.Set("uuid", puuid)
	return puuid
}

// Sets the attribute with the given value, takes two inputs the first is the
// attribute name and the second is the attribute value.  It returns the
// attributes for function stacking.
func (h *Attributes) Set(name, val string) *Attributes {
	attrs := []Attribute(*h)
	for i := range attrs {
		if attrs[i].Name == name {
			attrs[i].Value = val
			return h
		}
	}
	out := make([]Attribute, len(attrs)+1)
	copy(out, attrs)
	out[len(attrs)] = Attribute{name, val}
	*h = Attributes(out)
	return h
}

// Parse the FlowFile attributes from binary.
func (h *Attributes) Unmarshall(in io.Reader) (err error) {
	{
		hdr := make([]byte, 7)
		if _, err = in.Read(hdr); err != nil {
			if err == http.ErrBodyReadAfterClose || err == io.EOF {
				return io.EOF
			}
			return fmt.Errorf("Error reading NiFiFF3 header: %s", err)
		}
		if string(hdr) != "NiFiFF3" {
			return fmt.Errorf("No NiFiFF3 header found")
		}
	}

	var attrCount, size uint16
	if err = binary.Read(in, binary.BigEndian, &attrCount); err != nil {
		return fmt.Errorf("Error reading attrCount: %s", err)
	}
	for i := uint16(0); i < attrCount; i++ {
		if err = binary.Read(in, binary.BigEndian, &size); err != nil {
			return fmt.Errorf("Error reading attrName size: %s", err)
		}
		attrName := make([]byte, size)
		if _, err = in.Read(attrName); err != nil {
			return fmt.Errorf("Error reading attrName: %s", err)
		}
		if err = binary.Read(in, binary.BigEndian, &size); err != nil {
			return fmt.Errorf("Error reading attrValue size: %s", err)
		}
		attrValue := make([]byte, size)
		if _, err = in.Read(attrValue); err != nil {
			return fmt.Errorf("Error reading attrValue: %s", err)
		}
		h.Set(string(attrName), string(attrValue))
	}
	return
}

// Parse the FlowFile attributes into binary.
func (h *Attributes) Marshall(out io.Writer) (err error) {
	if _, err = out.Write([]byte("NiFiFF3")); err != nil {
		return fmt.Errorf("Error writing NiFiFF3 header: %s", err)
	}
	var (
		attrs     = []Attribute(*h)
		attrCount = uint16(len(attrs))
		size      uint16
	)
	if err = binary.Write(out, binary.BigEndian, attrCount); err != nil {
		return fmt.Errorf("Error writing attrCount: %s", err)
	}
	for i := uint16(0); i < attrCount; i++ {
		size = uint16(len(attrs[i].Name))
		if err = binary.Write(out, binary.BigEndian, size); err != nil {
			return fmt.Errorf("Error writing attrName size: %s", err)
		}
		if _, err = out.Write([]byte(attrs[i].Name[:size])); err != nil {
			return fmt.Errorf("Error writing attrName: %s", err)
		}

		size = uint16(len(attrs[i].Value))
		if err = binary.Write(out, binary.BigEndian, size); err != nil {
			return fmt.Errorf("Error writing attrValue size: %s", err)
		}
		if _, err = out.Write([]byte(attrs[i].Value[:size])); err != nil {
			return fmt.Errorf("Error writing attrValue: %s", err)
		}
	}
	return
}
