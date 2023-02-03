package flowfile

import (
	"bufio"
	"encoding/binary"
	"io"
	"log"
)

// Send out a set of flowfiles over the wire
func SendFiles(out io.Writer, ff []*File) (err error) {
	bw := bufio.NewWriter(out)
	defer bw.Flush()
	for _, f := range ff {
		if err = f.Attrs.Marshal(bw); err != nil {
			if Debug {
				log.Println("Failed to send size header", err)
			}
			return
		}

		if err = binary.Write(bw, binary.BigEndian, uint64(f.n)); err != nil {
			if Debug {
				log.Println("Failed to send size header", err)
			}
			return
		}

		if _, err = io.Copy(bw, f); err == io.EOF {
			err = nil
		} else if err != nil {
			if Debug {
				log.Println("Failed to send contents", err)
			}
			return
		}
	}
	return
}
