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
	lastSend      time.Time

	RetryCount int // When using a ReadAt reader, attempt multiple retries
	RetryDelay time.Duration

	tlsConfig  *tls.Config
	clientPool sync.Pool

	// Non-standard NiFi entities supported by this library
	MaxPartitionSize int64  // Maximum partition size for partitioned file
	CheckSumType     string // What kind of CheckSum to use for sent files

	hold *bool
}

// Create the HTTP sender and verify that the remote side is listening.
func NewHTTPTransaction(url string, cfg *tls.Config) (*HTTPTransaction, error) {
	var tlsConfig *tls.Config
	if cfg != nil {
		tlsConfig = cfg.Clone() // Create a copy for immutability
	}

	hs := &HTTPTransaction{
		url:          url,
		tlsConfig:    cfg,
		CheckSumType: "SHA256",
	}
	hs.clientPool = sync.Pool{
		New: func() any {
			return &http.Client{
				Timeout: 30 * time.Second,
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
					TLSClientConfig:       tlsConfig,
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

	client := hs.clientPool.Get().(*http.Client)
	defer hs.clientPool.Put(client)

	//if Debug {
	//	log.Printf("HTTP.Client: %#v\n", *client)
	//}

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
		hs.lastSend = time.Now()
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
	} else {
		hs.MaxPartitionSize = 0
	}

	hs.TransactionID, hs.Server = txid, res.Header.Get("Server")
	return nil
}

// Send one or more flow files to the remote server and return any errors back.
// A nil return for error is a successful send.
//
// This method of sending will make one POST-per-file which is not recommended
// for small files.  To increase throughput on smaller files one should
// consider using either NewHTTPPostWriter or NewHTTPBufferedPostWriter.
func (hs *HTTPTransaction) doSend(ff ...*File) (err error) {
	httpWriter := hs.NewHTTPBufferedPostWriter()
	defer func() {
		closeErr := httpWriter.Close()
		if closeErr != nil {
			err = closeErr
		}
		if httpWriter.Response == nil {
			err = fmt.Errorf("File did not send, no response")
		} else if httpWriter.Response.StatusCode != 200 {
			err = fmt.Errorf("File did not send successfully, code %d", httpWriter.Response.StatusCode)
		}
	}()
	for i, f := range ff {
		if Debug {
			fmt.Printf("  sending item #%d\n", i)
		}
		_, err = httpWriter.Write(f)
		if err != nil {
			if Debug {
				fmt.Println("write err:", err)
			}
			httpWriter.Terminate()
			return
		}
	}
	return
}

// Send one or more flow files to the remote server and return any errors back.
// A nil return for error is a successful send.
//
// A failed send will be retried if HTTPTransaction.RetryCount is set and the File
// uses a ReadAt reader, a (1+retries) attempts will be made with a HTTPTransaction.RetryDelay between retries.
//
//   // With one or more files:
//   err = hs.Send(file1)
//   err = hs.Send(file1, file2) // or more
//   // A slice of files:
//   err = hs.Send(files...)
//
// This method of sending will make one POST-per-file which is not recommended
// for small files.  To increase throughput on smaller files one should
// consider using either NewHTTPPostWriter or NewHTTPBufferedPostWriter.
func (hs *HTTPTransaction) Send(ff ...*File) (err error) {
	if len(ff) == 0 {
		return
	}
	// do the work, give up after first try if retry is not enabled
	if err = hs.doSend(ff...); err == nil || hs.RetryCount <= 0 {
		return
	}

	// Verify we can send with retries, the sender must be resettable
	for _, f := range ff {
		if err = f.Reset(); err != nil {
			return
		}
	}

	// Loop over our tries
	for try := 1; err != nil && try < hs.RetryCount; try++ {
		if Debug {
			log.Printf("Retrying send %d, %s\n", try, err)
		}

		// do the work
		if err = hs.doSend(ff...); err == nil {
			break
		}

		// hold off, handshake, and retry
		time.Sleep(hs.RetryDelay)

		// For sanity, we should handshake to get a new transaction id
		hs.Handshake()
	}
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
	pw            *io.PipeWriter

	client    *http.Client
	clientErr chan error
	Response  *http.Response

	writeLock sync.Mutex
	init      func()
}

// Write a flow file to the remote server and return any errors back.  One
// cannot determine if there has been a successful send until the HTTPPostWriter is
// closed.  Then the Response.StatusCode will be set with the reply from the
// server.
func (hw *HTTPPostWriter) Write(f *File) (n int64, err error) {
	// Make sure only one is being written to the stream at once
	hw.writeLock.Lock()
	defer hw.writeLock.Unlock()

	defer func() {
		if Debug && err != nil {
			fmt.Println("write err:", err)
		}
	}()

	// On first write, initaite the POST
	if hw.init != nil {
		hw.init()
		hw.init = nil
	}

	if hw.client == nil {
		err = fmt.Errorf("HTTPTransaction Closed")
		return
	}

	if f.Size > 0 && f.Attrs.Get("checksumType") == "" {
		f.AddChecksum(hw.hs.CheckSumType)
	}
	n, err = writeTo(hw.w, f, 0)
	hw.Sent += n
	return
}

// Close the HTTPPostWriter and flush the data to the stream
func (hw *HTTPPostWriter) Close() (err error) {
	hw.writeLock.Lock()
	defer hw.writeLock.Unlock()
	hw.w.Close()
	hw.w = nil

	if Debug {
		log.Println("closed channel, waiting for post reply")
	}
	err = <-hw.clientErr
	if Debug {
		log.Println("replied!")
	}

	hw.hs.clientPool.Put(hw.client)
	hw.client = nil
	return err
}

// Terminate the HTTPPostWriter
func (hw *HTTPPostWriter) Terminate() {
	hw.pw.CloseWithError(fmt.Errorf("Post Terminated"))
}

// NewHTTPPostWriter creates a POST to a NiFi listening endpoint and allows
// multiple files to be written to the endpoint at one time.  This reduces
// additional overhead (with fewer HTTP responses) and decreases latency (by
// instead putting pressure on TCP with smaller payload sizes).
//
// However, HTTPPostWriter increases the chances of failures as all the sent
// files will be marked as failed if the the HTTP POST is not a success.
func (hs *HTTPTransaction) NewHTTPPostWriter() (httpWriter *HTTPPostWriter) {

	client := hs.clientPool.Get().(*http.Client)

	if Debug {
		log.Printf("HTTP.Client: %#v\n", *client)
	}

	r, w := io.Pipe()
	httpWriter = &HTTPPostWriter{
		Header:    make(http.Header),
		pw:        w,
		w:         w,
		hs:        hs,
		client:    client,
		clientErr: make(chan error),
	}
	httpWriter.init = func() {
		go httpWriter.doPost(hs, r)
	}
	return
}

// NewHTTPBufferedPostWriter creates a POST to a NiFi listening endpoint and
// allows multiple files to be written to the endpoint at one time.  This
// reduces additional overhead (with fewer HTTP responses) and decreases
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
		pw:            w,
		w:             mlw,
		hs:            hs,
		FlushInterval: 400 * time.Millisecond,
		client:        client,
		clientErr:     make(chan error),
	}

	httpWriter.init = func() {
		mlw.latency = httpWriter.FlushInterval
		go mlw.flushLoop()
		go httpWriter.doPost(hs, r)
	}
	return
}

func (httpWriter *HTTPPostWriter) doPost(hs *HTTPTransaction, r io.ReadCloser) {
	err := fmt.Errorf("POST did not complete")
	defer func() {
		r.Close() // Make sure pipe is terminated
		httpWriter.clientErr <- err
	}()

	req, _ := http.NewRequest("POST", hs.url, r)
	// We shouldn't get an error here as the session would have already
	// established the connection details.

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
	httpWriter.Response, err = httpWriter.client.Do(req)
	if Debug {
		log.Println("POST response:", httpWriter.Response, err)
	}
}
