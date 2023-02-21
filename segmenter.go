package flowfile // import "github.com/pschou/go-flowfile"

import (
	"fmt"
	"os"
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
	if in.ra == nil && in.filePath == "" {
		return nil, fmt.Errorf("Must have a reader with ReadAt capabilities to segment")
	}

	size := in.Size
	//fmt.Println("size", size, "segment", segmentSize)
	if segmentSize == 0 || size < segmentSize {
		return []*File{in}, nil
	}
	count := int((size-1)/segmentSize + 1)

	baseAttrs := in.Attrs.Clone()

	// Make sure uuid is set
	if uuid := baseAttrs.Get("uuid"); uuid == "" {
		baseAttrs.GenerateUUID()
	}
	baseAttrs.Set("fragment.identifier", baseAttrs.Get("uuid"))
	baseAttrs.Unset("uuid")

	baseAttrs.Set("segment.original.size", fmt.Sprintf("%d", size))
	baseAttrs.Set("segment.original.filename", in.Attrs.Get("filename"))
	if ct := in.Attrs.Get("checksumType"); ct != "" {
		baseAttrs.Set("segment.original.checksumType", ct)
		baseAttrs.Set("segment.original.checksum", in.Attrs.Get("checksum"))
		baseAttrs.Unset("checksumType")
		baseAttrs.Unset("checksum")
	}

	st, en := int64(0), in.i
	for i := 0; i < count; i++ {
		st, en = en, en+segmentSize
		if en > size {
			en = size
		}

		if in.fileAutoOpen { // Make sure the file is closed if auto opened
			in.fileAutoOpen = false
			fh := in.ra.(*os.File)
			in.ra = nil
			fh.Close()
		}

		f := &File{
			ra:       in.ra,
			filePath: in.filePath,
			i:        st,
			Size:     en - st,
			n:        en - st,
			Attrs:    baseAttrs.Clone(),
		}
		f.Attrs.Set("merge.reason", "MAX_BYTES_THRESHOLD_REACHED")
		f.Attrs.Set("fragment.offset", fmt.Sprintf("%d", st))
		f.Attrs.Set("fragment.index", fmt.Sprintf("%d", i+1))
		f.Attrs.Set("fragment.count", fmt.Sprintf("%d", count))
		f.Attrs.GenerateUUID()
		out = append(out, f)
	}
	in.ra, in.i, in.n = nil, in.i-in.n, 0
	return
}
