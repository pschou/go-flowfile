package flowfile

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
)

type HTTPReciever struct {
	Handler          func(*File, *http.Request) error
	BytesSeen        uint64
	Server           string
	MaxPartitionSize uint64
	ErrorCorrection  float64
}

// Handle for accepting flow files through a http webserver.  The handle here
// is intended to be used in a Listen Handler so as to make building out all
// the web endpoints seemless.
//
//  ffReciever := flowfile.HTTPReciever{Handler: post}
//  http.Handle("/contentListener", ffReciever)
//  log.Fatal(http.ListenAndServe(":8080", nil))
//
func (f HTTPReciever) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if f.Handler == nil {
		w.WriteHeader(http.StatusNotImplemented)
		return
	}
	hdr := w.Header()
	switch r.Method {
	case "HEAD":
		// Handle the head request method
		hdr.Set("Accept", "application/flowfile-v3,*/*;q=0.8")
		if f.MaxPartitionSize > 0 {
			hdr.Set("x-ff-max-partition-size", fmt.Sprintf("%d", f.MaxPartitionSize))
		}
		hdr.Set("x-nifi-transfer-protocol-version", "3")
		hdr.Set("Content-Length", "0")
		if f.Server != "" {
			hdr.Set("Server", f.Server)
		}
		w.WriteHeader(http.StatusOK)

	case "POST":
		// Handle the post request method
		Body := r.Body
		var err error
		defer func() {
			io.Copy(ioutil.Discard, Body)
			Body.Close()
			hdr.Set("Content-Type", "text/plain")
			hdr.Set("Content-Length", "0")
			if f.Server != "" {
				hdr.Set("Server", f.Server)
			}
			if err == nil {
				w.WriteHeader(http.StatusOK)
			} else {
				log.Printf("Error: %s", err)
				http.Error(w, fmt.Sprintf("Error %s", err), http.StatusInternalServerError)
			}
		}()

		switch ct := strings.ToLower(r.Header.Get("Content-Type")); ct {
		case "application/flowfile-v3":
			for err == nil {
				// Loop over files as long as there is more data available:
				var a Attributes
				err = a.Unmarshall(Body)
				if err != nil {
					if err == io.EOF {
						err = nil
					}
					break
				}

				var N uint64
				if err = binary.Read(Body, binary.BigEndian, &N); err != nil {
					err = fmt.Errorf("Error parsing file size: %s", err)
					return
				}

				ff := &File{r: Body, n: int64(N), Attrs: a}
				ff.cksumInit()
				r.Body = ff
				err = f.Handler(ff, r)
				ff.Close()
				if ff.closed {
					break
				}
			}
		default:
			if N, err := strconv.ParseUint(r.Header.Get("Content-Length"), 10, 64); err == nil {
				ff := &File{r: Body, n: int64(N)}
				r.Body = ff
				f.Handler(ff, r)
				ff.Close()
			}
		}
	}
}
