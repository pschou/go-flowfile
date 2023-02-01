package flowfile

import (
	"bufio"
	"encoding/binary"
	"io"
)

// Send out a set of flowfiles over the wire
func SendFiles(out io.Writer, ff []*File) (err error) {
	bw := bufio.NewWriter(out)
	defer bw.Flush()
	for _, f := range ff {
		f.Attrs.Marshall(bw)

		if err = binary.Write(bw, binary.BigEndian, uint64(f.n)); err != nil {
			return
		}

		if _, err = io.Copy(bw, f); err == io.EOF {
			err = nil
		} else if err != nil {
			return
		}
	}
	return
}
