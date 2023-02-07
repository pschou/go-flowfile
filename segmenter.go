package flowfile // import "github.com/pschou/go-flowfile"

import (
	"fmt"
)

// Splits up a flowfile into count number of segments.  The intended purpose
// here is to enable larger files to be sent in smaller chucks so as to avoid
// having to replay sending a whole file in case a connection gets dropped.
func Segment(in *File, count int64) (out []*File, err error) {
	size := in.n
	segmentSize := size / count
	if size%count > 0 {
		segmentSize++
	}
	return SegmentBySize(in, segmentSize)
}

// Splits up a flowfile into a number of segments with segmentSize.  The intended
// purpose here is to enable larger files to be sent in smaller chucks so as to
// avoid having to replay sending a whole file in case a connection gets
// dropped.
func SegmentBySize(in *File, segmentSize int64) (out []*File, err error) {
	if in.ra == nil {
		return nil, fmt.Errorf("Must have a reader with ReadAt capabilities to segment")
	}

	size := in.Size
	//fmt.Println("size", size, "segment", segmentSize)
	if segmentSize == 0 || size < segmentSize {
		return []*File{in}, nil
	}
	count := int((size-1)/segmentSize + 1)

	// Make sure parent attributes are set
	puuid := in.Attrs.Get("uuid")
	if puuid == "" {
		puuid = in.Attrs.GenerateUUID()
	}
	in.Attrs.Set("size", fmt.Sprintf("%d", size))

	// Create labeling for the child segment
	attrs := []Attribute(in.Attrs)
	parentAttrs := make([]Attribute, len(attrs))
	for i := range attrs {
		switch attrs[i].Name {
		case "filename", "path":
			parentAttrs[i].Name = attrs[i].Name
		default:
			parentAttrs[i].Name = "parent-" + attrs[i].Name
		}
		parentAttrs[i].Value = attrs[i].Value
	}

	st, en := int64(0), in.i+segmentSize
	for i := 0; i < count; i++ {
		st, en = en, en+segmentSize
		if en > size {
			en = size
		}

		f := &File{
			ra:    in.ra,
			i:     st,
			Size:  en - st,
			n:     en - st,
			Attrs: Attributes(parentAttrs).Clone(),
		}
		f.Attrs.Set("segment-offset", fmt.Sprintf("%d", st))
		f.Attrs.Set("segment-index", fmt.Sprintf("%d", i))
		f.Attrs.Set("segment-count", fmt.Sprintf("%d", count))
		f.Attrs.GenerateUUID()
		out = append(out, f)
	}
	in.ra, in.n = nil, 0
	return
}
