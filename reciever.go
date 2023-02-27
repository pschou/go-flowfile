package flowfile // import "github.com/pschou/go-flowfile"

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

// Implements http.Handler and can be used with the GoLang built-in http module:
//   https://pkg.go.dev/net/http#Handler
type HTTPReceiver struct {
	Server           string
	MaxPartitionSize int64

	connections    int
	MaxConnections int

	Metrics *Metrics
	handler func(*Scanner, http.ResponseWriter, *http.Request)
}

// NewHTTPReceiver interfaces with the built-in HTTP Handler and parses out the
// FlowFile stream and provids a FlowFile scanner to a FlowFile handler.
func NewHTTPReceiver(handler func(*Scanner, http.ResponseWriter, *http.Request)) *HTTPReceiver {
	return &HTTPReceiver{
		handler: handler,
		Metrics: NewMetrics(),
	}
}

// NewHTTPFileReceiver interfaces with the built-in HTTP Handler and parses out
// the individual FlowFiles from a stream and sends them to a FlowFile handler.
func NewHTTPFileReceiver(handler func(*File, http.ResponseWriter, *http.Request) error) *HTTPReceiver {
	return &HTTPReceiver{
		handler: func(s *Scanner, w http.ResponseWriter, r *http.Request) {
			for s.Scan() {
				if err := handler(s.File(), w, r); err != nil {
					w.WriteHeader(http.StatusNotAcceptable)
					return
				}
			}
			if err := s.Err(); err == nil || err == io.EOF {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
			}
			return
		},
		Metrics: NewMetrics(),
	}
}

// Handle for accepting flow files through a http webserver.  The handle here
// is intended to be used in a Listen Handler so as to make building out all
// the web endpoints seemless.
//
//  ffReceiver := flowfile.HTTPReceiver{Handler: post}
//  http.Handle("/contentListener", ffReceiver)
//  log.Fatal(http.ListenAndServe(":8080", nil))
//
func (f *HTTPReceiver) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// What to do if the creation was not done correctly
	if f.handler == nil {
		w.WriteHeader(http.StatusNotImplemented)
		return
	}

	f.Metrics.MetricsThreadsQueued += 1
	var once sync.Once
	var active bool
	doOnce := func() {
		f.Metrics.MetricsThreadsQueued -= 1
		f.Metrics.MetricsThreadsActive += 1
		active = true
	}
	defer func() {
		once.Do(doOnce)
		if active {
			f.Metrics.MetricsThreadsActive -= 1
		} else {
			f.Metrics.MetricsThreadsQueued -= 1
		}
		f.Metrics.MetricsThreadsTerminated += 1
	}()

	// What to do if we are busy!
	f.connections++
	defer func() { f.connections-- }()
	if f.MaxConnections > 0 && f.connections >= f.MaxConnections {
		if Debug {
			log.Println("Denying connection as MaxConnections has been met")
		}
		http.Error(w, "503 too busy", http.StatusServiceUnavailable)
		return
	}

	hdr := w.Header()
	switch r.Method {
	case "HEAD":
		// Handle the head request method
		hdr.Set("Accept", "application/flowfile-v3")
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
		defer func() {
			io.Copy(ioutil.Discard, Body)
			Body.Close()
			hdr.Set("Content-Type", "text/plain")
			hdr.Set("Content-Length", "0")
			if f.Server != "" {
				hdr.Set("Server", f.Server)
			}
		}()

		switch ct := strings.ToLower(r.Header.Get("Content-Type")); ct {
		case "application/flowfile-v3":
			reader := &Scanner{r: Body, every: func(ff *File) {
				once.Do(doOnce)
				f.Metrics.BucketCounter(ff.Size)
			}}
			f.handler(reader, w, r)
			reader.Close()
			if reader.err != nil {
				if Debug && reader.Err() != nil {
					log.Printf("Scanner Error: %s", reader.err)
				}
				return
			}
		default:
			if N, err := strconv.ParseUint(r.Header.Get("Content-Length"), 10, 64); err == nil {
				ch := make(chan *File, 1)
				ch <- &File{r: Body, n: int64(N)}
				reader := &Scanner{ch: ch, every: func(ff *File) {
					once.Do(doOnce)
					f.Metrics.BucketCounter(ff.Size)
				}}
				f.handler(reader, w, r)
				reader.Close()
			}
		}
	}
}
