package flowfile // import "github.com/pschou/go-flowfile"

import (
	"bufio"
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// The HTTP Sender will establish a NiFi handshake and ensure that the remote
// endpoint is listening and compatible with the current flow file format.
type HTTPTransaction struct {
	url           string
	Server        string
	TransactionID string

	// Create an upper bound for threading
	MaxClients, clients int

	tlsConfig  *tls.Config
	clientPool sync.Pool

	// Non-standard NiFi entities supported by this library
	MaxPartitionSize int64  // Maximum partition size for partitioned file
	CheckSumType     string // What kind of CheckSum to use for sent files

	hold *bool
	//waitGroup sizedwaitgroup.SizedWaitGroup
	wait sync.RWMutex
}

// Create the HTTP sender and verify that the remote side is listening.
func NewHTTPTransaction(url string, cfg *tls.Config) (*HTTPTransaction, error) {
	tlsConfig := cfg.Clone() // Create a copy for immutability

	hs := &HTTPTransaction{
		url:          url,
		tlsConfig:    cfg,
		CheckSumType: "SHA256",
	}
	hs.clientPool = sync.Pool{
		New: func() any {
			return &http.Client{
				Transport: &http.Transport{
					Proxy: http.ProxyFromEnvironment,
					DialContext: (&net.Dialer{
						Timeout:   30 * time.Second,
						KeepAlive: 30 * time.Second,
					}).DialContext,
					ForceAttemptHTTP2:     true,
					MaxIdleConns:          100,
					IdleConnTimeout:       90 * time.Second,
					TLSHandshakeTimeout:   10 * time.Second,
					ExpectContinueTimeout: 1 * time.Second,
					TLSClientConfig:       tlsConfig.Clone(),
				},
			}
		}}

	err := hs.Handshake()
	if err != nil {
		return nil, err
	}
	return hs, nil
}

// Establishes or re-establishes a transaction id with NiFi to begin the
// process of transferring flowfiles.  This is a blocking call so no new files
// will be sent until this is completed.
func (hs *HTTPTransaction) Handshake() error {
	hs.wait.Lock()
	defer hs.wait.Unlock()

	client := hs.clientPool.Get().(*http.Client)
	defer hs.clientPool.Put(client)

	req, err := http.NewRequest("HEAD", hs.url, nil)
	if err != nil {
		return err
	}

	txid := uuid.New().String()
	req.Header.Set("x-nifi-transaction-id", txid)
	req.Header.Set("Connection", "Keep-alive")
	req.Header.Set("User-Agent", UserAgent)
	res, err := client.Do(req)
	if err != nil {
		return err
	}

	if Debug {
		log.Printf("Result on query: %#v\n", res)
	}

	switch res.StatusCode {
	case 200: // Success
	case 405:
		return fmt.Errorf("Method not allowed, make sure the remote server accepts flowfile-v3")
	default:
		return fmt.Errorf("Unexpected status code %d", res.StatusCode)
	}

	// If the initial post was redirected, we'll want to stick with the final URL
	hs.url = res.Request.URL.String()

	{ // Check for Accept types
		types := strings.Split(res.Header.Get("Accept"), ",")
		var hasFF bool
		for _, t := range types {
			if strings.HasPrefix(t, "application/flowfile-v3") {
				hasFF = true
				break
			}
		}
		if !hasFF {
			return fmt.Errorf("Server does not support flowfile-v3")
		}
	}

	// Check for protocol version
	switch v := res.Header.Get("x-nifi-transfer-protocol-version"); v {
	case "3": // Add more versions after verifying support is there
	default:
		return fmt.Errorf("Unknown NiFi TransferVersion %q", v)
	}

	// Parse out non-standard fields
	if v := res.Header.Get("Max-Partition-Size"); v != "" {
		maxPartitionSize, err := strconv.ParseUint(v, 10, 64)
		if err == nil {
			hs.MaxPartitionSize = int64(maxPartitionSize)
		} else {
			if Debug {
				log.Println("Unable to parse Max-Partition-Size", err)
			}
		}
	}

	hs.TransactionID, hs.Server = txid, res.Header.Get("Server")
	return nil
}

// Send a flow file to the remote server and return any errors back.
// A nil return for error is a successful send.
//
// This method of sending will make one POST-per-file which is not recommended
// for small files.  To increase throughput on smaller files one should
// consider using either NewHTTPPostWriter or NewHTTPBufferedPostWriter.
func (hs *HTTPTransaction) Send(f *File) (err error) {
	httpWriter := hs.NewHTTPPostWriter()
	defer func() {
		httpWriter.Close()
		if httpWriter.Response == nil {
			err = fmt.Errorf("File did not send, no response")
		} else if httpWriter.Response.StatusCode != 200 {
			err = fmt.Errorf("File did not send successfully, code %d", httpWriter.Response.StatusCode)
		}
	}()
	_, err = httpWriter.Write(f)
	return
}

// Set a header value which is configured to be sent
func (hs *HTTPTransaction) Close() (err error) {
	hs.wait.Lock()
	defer hs.wait.Unlock()

	hs.TransactionID, hs.Server = "", ""
	return
}

// Writer ecapsulates the ability to write one or more flow files in one POST
// request.  This must be closed upon completion of the last File sent.
//
// One needs to first create an HTTPTransaction before one can create an
// HTTPPostWriter, so the process looks like:
//
//   ff1 := flowfile.New(strings.NewReader("test1"), 5)
//   ff2 := flowfile.New(strings.NewReader("test2"), 5)
//   ht, err := flowfile.NewHTTPTransaction("http://localhost:8080/contentListener", http.DefaultClient)
//   if err != nil {
//     log.Fatal(err)
//   }
//
//   w := ht.NewHTTPPostWriter() // Create the POST to the NiFi endpoint
//   w.Write(ff1)
//   w.Write(ff2)
//   err = w.Close() // Finalize the POST
type HTTPPostWriter struct {
	Header        http.Header
	FlushInterval time.Duration
	Sent          int64
	hs            *HTTPTransaction
	w             io.WriteCloser
	err           error

	client    *http.Client
	clientErr error

	Response *http.Response

	writeLock, replyLock sync.Mutex
	init                 func()
}

// Write a flow file to the remote server and return any errors back.  One
// cannot determine if there has been a successful send until the HTTPPostWriter is
// closed.  Then the Response.StatusCode will be set with the reply from the
// server.
func (hw *HTTPPostWriter) Write(f *File) (n int64, err error) {
	if hw.client == nil {
		err = fmt.Errorf("HTTPTransaction Closed")
		return
	}

	// On first write, initaite the POST
	if hw.init != nil {
		hw.replyLock.Lock()
		hw.init()
		hw.init = nil
	}

	// Make sure only one is being written to the stream at once
	hw.writeLock.Lock()
	defer hw.writeLock.Unlock()

	if hw.clientErr != nil {
		err = hw.clientErr
		return
	}
	if f.Size > 0 && f.Attrs.Get("checksumType") == "" {
		f.AddChecksum(hw.hs.CheckSumType)
	}
	n, err = writeTo(hw.w, f)
	hw.Sent += n
	return
}

// Close the HTTPPostWriter and flush the data to the stream
func (hw *HTTPPostWriter) Close() (err error) {
	hw.w.Close()
	hw.replyLock.Lock()
	hw.replyLock.Unlock()
	hw.hs.clientPool.Put(hw.client)
	hw.client = nil
	return hw.clientErr
}

// NewHTTPPostWriter creates a POST to a NiFi listening endpoint and allows
// multiple files to be written to the endpoint at one time.  This reduces
// additional overhead (with fewer HTTP reponses) and decreases latency (by
// instead putting pressure on TCP with smaller payload sizes).
//
// However, HTTPPostWriter increases the chances of failures as all the sent
// files will be marked as failed if the the HTTP POST is not a success.
func (hs *HTTPTransaction) NewHTTPPostWriter() (httpWriter *HTTPPostWriter) {

	client := hs.clientPool.Get().(*http.Client)

	r, w := io.Pipe()
	httpWriter = &HTTPPostWriter{
		Header: make(http.Header),
		w:      w,
		hs:     hs,
		init: func() {
			go doPost(hs, httpWriter, r)
		},
		client: client,
	}
	return
}

// NewHTTPBufferedPostWriter creates a POST to a NiFi listening endpoint and
// allows multiple files to be written to the endpoint at one time.  This
// reduces additional overhead (with fewer HTTP reponses) and decreases
// latency.  Additionally, the added buffering helps with constructing larger
// packets, thus further reducing TCP overhead.
//
// However, HTTPPostWriter increases the chances of failures as all the sent
// files will be marked as failed if the the HTTP POST is not a success.
func (hs *HTTPTransaction) NewHTTPBufferedPostWriter() (httpWriter *HTTPPostWriter) {
	r, w := io.Pipe()
	mlw := &maxLatencyWriter{
		dst:  bufio.NewWriter(w),
		c:    w,
		done: make(chan bool),
	}

	client := hs.clientPool.Get().(*http.Client)

	httpWriter = &HTTPPostWriter{
		Header:        make(http.Header),
		w:             mlw,
		hs:            hs,
		FlushInterval: 400 * time.Millisecond,
		client:        client,
	}
	httpWriter.init = func() {
		mlw.latency = httpWriter.FlushInterval
		go mlw.flushLoop()
		go doPost(hs, httpWriter, r)
	}
	return
}

func doPost(hs *HTTPTransaction, httpWriter *HTTPPostWriter, r io.ReadCloser) {
	hs.wait.RLock()
	defer hs.wait.RUnlock()

	req, _ := http.NewRequest("POST", hs.url, r)
	// We shouldn't get an error here as the session would have already
	// established the connection details.

	defer httpWriter.replyLock.Unlock()
	defer r.Close() // Make sure pipe is terminated
	// Set custom http headers
	if httpWriter.Header != nil {
		for k, v := range httpWriter.Header {
			if len(v) > 0 {
				req.Header.Set(k, v[0])
			}
		}
	}

	req.Header.Set("Content-Type", "application/flowfile-v3")
	req.Header.Set("x-nifi-transfer-protocol-version", "3")
	req.Header.Set("x-nifi-transaction-id", hs.TransactionID)
	req.Header.Set("Transfer-Encoding", "chunked")
	req.Header.Set("Connection", "Keep-alive")
	req.Header.Set("User-Agent", UserAgent)
	//if Debug {
	//	log.Println("doing request", req)
	//}
	httpWriter.Response, httpWriter.clientErr = httpWriter.client.Do(req)
	if Debug {
		log.Println("set reponse", httpWriter.Response, httpWriter.clientErr)
	}
}
