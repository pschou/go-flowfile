package flowfile

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
)

type HTTPReciever struct {
	Server           string
	MaxPartitionSize int
	ErrorCorrection  float64

	handler func(*Scanner, *http.Request) error
	//BytesSeen        uint64
}

// NewHTTPReciever interfaces with the built-in HTTP Handler and parses out the
// FlowFile stream and provids a FlowFile scanner to a FlowFile handler.
func NewHTTPReciever(handler func(*Scanner, *http.Request) error) *HTTPReciever {
	return &HTTPReciever{handler: handler}
}

// NewHTTPFileReciever interfaces with the built-in HTTP Handler and parses out
// the individual FlowFiles from a stream and sends them to a FlowFile handler.
func NewHTTPFileReciever(handler func(*File, *http.Request) error) *HTTPReciever {
	return &HTTPReciever{handler: func(s *Scanner, r *http.Request) (err error) {
		var ff *File
		for s.Scan() {
			if ff, err = s.File(); err != nil {
				return
			}
			if err = handler(ff, r); err != nil {
				return
			}
		}
		if err == io.EOF {
			err = nil
		}
		return
	}}
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
	if f.handler == nil {
		w.WriteHeader(http.StatusNotImplemented)
		return
	}
	hdr := w.Header()
	switch r.Method {
	case "HEAD":
		// Handle the head request method
		hdr.Set("Accept", "application/flowfile-v3,*/*;q=0.8")
		if f.MaxPartitionSize > 0 {
			hdr.Set("max-partition-size", fmt.Sprintf("%d", f.MaxPartitionSize))
		}
		hdr.Set("x-nifi-transfer-protocol-version", "3")
		hdr.Set("Content-Length", "0")
		hdr.Set("Server", AboutString)
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
				if Debug {
					log.Printf("Error: %s", err)
				}
				w.WriteHeader(http.StatusInternalServerError)
				//http.Error(w, fmt.Sprintf("Error %s", err), http.StatusInternalServerError)
			}
		}()

		switch ct := strings.ToLower(r.Header.Get("Content-Type")); ct {
		case "application/flowfile-v3":
			reader := &Scanner{r: Body}
			if err = f.handler(reader, r); err != nil {
				return
			}
			reader.Close()
			if reader.err != nil {
				return
			}
		default:
			if N, err := strconv.ParseUint(r.Header.Get("Content-Length"), 10, 64); err == nil {
				reader := &Scanner{one: &File{r: Body, n: int64(N)}}
				if err = f.handler(reader, r); err != nil {
					return
				}
				reader.Close()
			}
		}
	}
}
