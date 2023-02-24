package flowfile // import "github.com/pschou/go-flowfile"

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"sort"
	"strings"

	"github.com/google/uuid"
	"github.com/pschou/go-sorting/numstr"
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
func (h *Attributes) Unset(name string) (ok bool) {
	out := []Attribute{}
	for _, elm := range []Attribute(*h) {
		if elm.Name == name {
			ok = true
		} else {
			out = append(out, elm)
		}
	}
	*h = Attributes(out)
	return
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

// Internal call for adding attributes without duplicate checks
func (h *Attributes) add(name, val string) {
	attrs := []Attribute(*h)
	attrs = append(attrs, Attribute{name, val})
	*h = Attributes(attrs)
}

// Sets the attribute with the given value, takes two inputs the first is the
// attribute name and the second is the attribute value.  It returns the
// attributes for function stacking.
func (h *Attributes) Set(name, val string) *Attributes {
	if name == "filename" {
		// Sanitize the filename to make sure malformed data is misused
		_, val = path.Split(val)
	}
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

// Return the size of the header for computations of the total flow file size.
//   Total Size = Header + Data
func (f File) HeaderSize() (n int) {
	attrs := []Attribute(f.Attrs)
	n += 17 + 4*len(attrs)
	for _, a := range attrs {
		n += len(a.Value) + len(a.Name)
	}
	return
}

// Parse the FlowFile attributes from a binary slice.
func (h *Attributes) UnmarshalBinary(in []byte) (err error) {
	*h = Attributes{}
	err = h.ReadFrom(bytes.NewBuffer(in))
	return
}

const (
	FlowFile3Header = "NiFiFF3"
	FlowFileEOF     = "NiFiEOF"
)

var (
	ErrorNoFlowFileHeader      = errors.New("No NiFiFF3 header found")
	ErrorInvalidFlowFileHeader = errors.New("Invalid of incomplete FlowFile header")
)

// Parse the FlowFile attributes from binary Reader.
func (h *Attributes) ReadFrom(in io.Reader) (err error) {
	{
		hdr := make([]byte, 7)
		if _, err = in.Read(hdr); err != nil {
			if err == http.ErrBodyReadAfterClose || err == io.EOF {
				return io.EOF
			}
			return ErrorInvalidFlowFileHeader
		}
		if string(hdr) == FlowFileEOF {
			return io.EOF
		} else if string(hdr) != FlowFile3Header {
			return ErrorNoFlowFileHeader
		}
	}

	var attrCount, size uint16
	if err = binary.Read(in, binary.BigEndian, &attrCount); err != nil {
		return ErrorInvalidFlowFileHeader
	}
	for i := uint16(0); i < attrCount; i++ {
		if err = binary.Read(in, binary.BigEndian, &size); err != nil {
			return ErrorInvalidFlowFileHeader
		}
		attrName := make([]byte, size)
		if _, err = in.Read(attrName); err != nil {
			return ErrorInvalidFlowFileHeader
		}
		if err = binary.Read(in, binary.BigEndian, &size); err != nil {
			return ErrorInvalidFlowFileHeader
		}
		attrValue := make([]byte, size)
		if _, err = in.Read(attrValue); err != nil {
			return ErrorInvalidFlowFileHeader
		}
		h.Set(string(attrName), string(attrValue))
	}
	return nil
}

// Parse the FlowFile attributes into binary slice.
func (h Attributes) MarshalBinary() ([]byte, error) {
	buf := bytes.NewBuffer([]byte{})
	h.WriteTo(buf)
	return buf.Bytes(), nil
}

// Print out the attributes as a JSON map in a string
//
// To make this easy to read, use the json.Indent package like this:
//
//   var out bytes.Buffer
//   attrs.Sort()  // Optional, sort the attributes by name
//   json.Indent(&out, []byte(attrs.String()), "", "  ")
//   fmt.Printf("attributes: %s\n", out.String())
//
//   attributes: {
//     "custodyChain.1.host": "myhost",
//     "custodyChain.7.time": "...",
//     "custodyChain.11.time": "...",
//     "filename": "abcd-efgh"
//   }

func (h Attributes) String() string {
	s := &strings.Builder{}
	s.WriteString("{")
	attrs := []Attribute(h)
	for i, nv := range attrs {
		if i > 0 {
			s.WriteString(",")
		}
		n, _ := json.Marshal(nv.Name)
		v, _ := json.Marshal(nv.Value)
		s.Write(n)
		s.WriteString(":")
		s.Write(v)
	}
	s.WriteString("}")
	return s.String()
}

// Provides a MarshalJSON interface
func (h Attributes) MarshalJSON() ([]byte, error) {
	return []byte(h.String()), nil
}

var ErrorUnmarshallingAttributes = errors.New("Error unmarshalling attributes")

func (h *Attributes) UnmarshalJSON(in []byte) error {
	dec := json.NewDecoder(bytes.NewReader(in))
	t, err := dec.Token()
	if d, ok := t.(json.Delim); (ok && d.String() != "{") || err != nil {
		return ErrorUnmarshallingAttributes
	}
	var vals []string
	var attrs []Attribute
	for dec.More() {
		if t, err = dec.Token(); err != nil {
			return err
		}
		switch v := t.(type) {
		case string:
			vals = append(vals, v)
		default:
			return ErrorUnmarshallingAttributes
		}
		if len(vals) == 2 {
			attrs = append(attrs, Attribute{Name: vals[0], Value: vals[1]})
			vals = nil
		}
	}
	t, err = dec.Token()
	if d, ok := t.(json.Delim); (ok && d.String() != "}") || err != nil {
		return ErrorUnmarshallingAttributes
	}
	*h = attrs
	return nil
}

// Sort the attributes by name
func (h *Attributes) Sort() {
	attrs := []Attribute(*h)
	sort.Slice(attrs, func(i, j int) bool { return numstr.LessThanFold(attrs[i].Name, attrs[j].Name) })
	*h = attrs
}

// Parse the FlowFile attributes into binary writer.
func (h *Attributes) WriteTo(out io.Writer) (err error) {
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
