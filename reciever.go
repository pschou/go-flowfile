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
	"time"
)

// Implements http.Handler and can be used with the GoLang built-in http module:
//   https://pkg.go.dev/net/http#Handler
type HTTPReceiver struct {
	Server           string
	MaxPartitionSize int64

	connections    int
	MaxConnections int

	// Custom buckets can be defined by setting new buckets before ingesting data
	// Note the BucketValues is always N+1 sized, as the last is overflow
	MetricsFlowFileTransferredBuckets      []int64
	MetricsFlowFileTransferredBucketValues []int64
	MetricsFlowFileTransferredSum          int64
	MetricsFlowFileTransferredCount        int64

	//MetricsFlowFileReceivedSum   *int64
	//MetricsFlowFileReceivedCount *int64
	MetricsThreadsActive     int64
	MetricsThreadsTerminated int64
	MetricsThreadsQueued     int64
	metricsInitTime          time.Time

	handler func(*Scanner, http.ResponseWriter, *http.Request)
}

// NewHTTPReceiver interfaces with the built-in HTTP Handler and parses out the
// FlowFile stream and provids a FlowFile scanner to a FlowFile handler.
func NewHTTPReceiver(handler func(*Scanner, http.ResponseWriter, *http.Request)) *HTTPReceiver {
	return &HTTPReceiver{
		handler: handler,
		MetricsFlowFileTransferredBuckets: []int64{
			1e2, 2.5e2, 1e3,
			2.5e3, 1e4, 2.5e4, 1e5,
			2.5e5, 1e6, 2.5e6, 1e7,
			2.5e7, 1e8, 2.5e8, 1e9},
		MetricsFlowFileTransferredBucketValues: make([]int64, 16),
		metricsInitTime:                        time.Now(),
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
		MetricsFlowFileTransferredBuckets: []int64{
			1e2, 2.5e2, 1e3,
			2.5e3, 1e4, 2.5e4, 1e5,
			2.5e5, 1e6, 2.5e6, 1e7,
			2.5e7, 1e8, 2.5e8, 1e9},
		MetricsFlowFileTransferredBucketValues: make([]int64, 16),
		metricsInitTime:                        time.Now(),
	}
}

func (hr *HTTPReceiver) MetricsHandler() http.Handler {
	return &metrics{hr: hr}
}

type metrics struct {
	hr *HTTPReceiver
}

func (m metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(m.hr.Metrics()))
}

func (f HTTPReceiver) Metrics(keyValuePairs ...string) string {
	var lbl, lblAdd string
	if len(keyValuePairs) > 1 {
		for i := 1; i < len(keyValuePairs); i += 2 {
			lblAdd += "," + fmt.Sprintf("%s=%q", keyValuePairs[i-1], keyValuePairs[i])
		}
		lbl = "{" + lblAdd[1:] + "}"
	}
	w := &strings.Builder{}
	tm := time.Now().UnixMilli()
	var bk string
	for i, v := range f.MetricsFlowFileTransferredBucketValues {
		if i < len(f.MetricsFlowFileTransferredBuckets) {
			bk = fmt.Sprintf("%d", f.MetricsFlowFileTransferredBuckets[i])
		} else {
			bk = "+Inf"
		}
		fmt.Fprintf(w, "flowfiles_transfered_bytes_bucket{le=%q%s} %d %d\n", bk, lblAdd, v, tm)
	}
	fmt.Fprintf(w, "flowfiles_transfered_bytes_sum%s %d %d\n",
		lbl, f.MetricsFlowFileTransferredSum, tm)
	fmt.Fprintf(w, "flowfiles_transfered_bytes_count%s %d %d\n",
		lbl, f.MetricsFlowFileTransferredCount, tm)
	fmt.Fprintf(w, "flowfiles_threads_active%s %d %d\n",
		lbl, f.MetricsThreadsActive, tm)
	fmt.Fprintf(w, "flowfiles_threads_terminated%s %d %d\n",
		lbl, f.MetricsThreadsTerminated, tm)
	fmt.Fprintf(w, "flowfiles_threads_queued%s %d %d\n",
		lbl, f.MetricsThreadsQueued, tm)
	fmt.Fprintf(w, "flowfiles_started%s %d %d\n",
		lbl, f.metricsInitTime.UnixMilli(), tm)
	return w.String()
}

func (f *HTTPReceiver) bucketCounter(size int64) {
	idx := 0
	for ; idx < len(f.MetricsFlowFileTransferredBuckets) &&
		f.MetricsFlowFileTransferredBuckets[idx] <= size; idx++ {
	}
	//if Debug {
	//	fmt.Println("bucket size", size, idx, "in", f.MetricsFlowFileTransferredBuckets)
	//}
	f.MetricsFlowFileTransferredBucketValues[idx] += 1
	f.MetricsFlowFileTransferredSum += size
	f.MetricsFlowFileTransferredCount += 1
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

	f.MetricsThreadsQueued += 1
	var once sync.Once
	var active bool
	doOnce := func() {
		f.MetricsThreadsQueued -= 1
		f.MetricsThreadsActive += 1
		active = true
	}
	defer func() {
		once.Do(doOnce)
		if active {
			f.MetricsThreadsActive -= 1
		} else {
			f.MetricsThreadsQueued -= 1
		}
		f.MetricsThreadsTerminated += 1
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
				f.bucketCounter(ff.Size)
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
					f.bucketCounter(ff.Size)
				}}
				f.handler(reader, w, r)
				reader.Close()
			}
		}
	}
}
