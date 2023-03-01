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
	OnRetry    func(ff []*File, retry int, err error)

	tlsConfig *tls.Config
	client    *http.Client

	// Non-standard NiFi entities supported by this library
	MaxPartitionSize int64  // Maximum partition size for partitioned file
	CheckSumType     string // What kind of CheckSum to use for sent files

	MetricsHandshakeLatency time.Duration

	hold *bool
}

// Create the HTTP sender and verify that the remote side is listening.
func NewHTTPTransactionWithTransport(url string, cfg *http.Transport) (*HTTPTransaction, error) {
	var transportConfig *http.Transport
	if cfg != nil {
		transportConfig = cfg.Clone() // Create a copy for immutability
	}

	hs := &HTTPTransaction{
		url:       url,
		tlsConfig: transportConfig.TLSClientConfig,
		//CheckSumType: "SHA256",
		client: &http.Client{
			//Timeout: 30 * time.Second,
			Transport: transportConfig.Clone(),
		},
	}

	err := hs.Handshake()
	if err != nil {
		return nil, err
	}
	return hs, nil
}

// Create the HTTP sender and verify that the remote side is listening.
func NewHTTPTransaction(url string, cfg *tls.Config) (*HTTPTransaction, error) {
	var tlsConfig *tls.Config
	if cfg != nil {
		tlsConfig = cfg.Clone() // Create a copy for immutability
	}

	hs := &HTTPTransaction{
		url:       url,
		tlsConfig: cfg,
		//CheckSumType: "SHA256",
		client: &http.Client{
			//Timeout: 30 * time.Second,
			Transport: &http.Transport{
				Proxy:       http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					//Timeout:   30 * time.Second,
					//KeepAlive: 30 * time.Second,
				}).DialContext,
				ForceAttemptHTTP2: true,
				MaxIdleConns:      30,
				//IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   30 * time.Second,
				ExpectContinueTimeout: 10 * time.Second,
				TLSClientConfig:       tlsConfig,
			}},
	}

	err := hs.Handshake()
	if err != nil {
		return nil, err
	}
	return hs, nil
}

// Create the HTTP sender without verifying remote is listening
func NewHTTPTransactionNoHandshake(url string, cfg *tls.Config) *HTTPTransaction {
	var tlsConfig *tls.Config
	if cfg != nil {
		tlsConfig = cfg.Clone() // Create a copy for immutability
	}

	hs := &HTTPTransaction{
		url:       url,
		tlsConfig: cfg,
		//CheckSumType: "SHA256",
		client: &http.Client{
			//Timeout: 30 * time.Second,
			Transport: &http.Transport{
				Proxy:       http.ProxyFromEnvironment,
				DialContext: (&net.Dialer{
					//Timeout:   30 * time.Second,
					//KeepAlive: 30 * time.Second,
				}).DialContext,
				ForceAttemptHTTP2: true,
				MaxIdleConns:      30,
				//IdleConnTimeout:       90 * time.Second,
				TLSHandshakeTimeout:   30 * time.Second,
				ExpectContinueTimeout: 10 * time.Second,
				TLSClientConfig:       tlsConfig,
			}},
	}

	return hs
}

// Establishes or re-establishes a transaction id with NiFi to begin the
// process of transferring flowfiles.  This is a blocking call so no new files
// will be sent until this is completed.
func (hs *HTTPTransaction) Handshake() error {
	req, err := http.NewRequest("HEAD", hs.url, nil)
	if err != nil {
		return err
	}

	txid := uuid.New().String()
	req.Header.Set("x-nifi-transaction-id", txid)
	req.Header.Set("Connection", "Keep-alive")
	req.Header.Set("User-Agent", UserAgent)
	tick := time.Now()
	res, err := hs.client.Do(req)
	if err != nil {
		return err
	}
	hs.MetricsHandshakeLatency = time.Now().Sub(tick)

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
	err = fmt.Errorf("File did not send, no response")
	defer func() {
		if httpWriter.w == nil {
			return
		}
		httpWriter.Close() // make sure everything is closed up
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
	if err = httpWriter.Close(); err != nil {
		return
	}
	if httpWriter.Response == nil {
		err = fmt.Errorf("File did not send, no response")
	} else if httpWriter.Response.StatusCode != 200 {
		err = fmt.Errorf("File did not send successfully, code %d", httpWriter.Response.StatusCode)
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

	// If retries are enabled, verify that the payload is resettable, error out early
	if hs.RetryCount > 0 {
		for _, f := range ff {
			if err = f.Reset(); err != nil {
				return
			}
		}
	}

	// do the work, give up after first try if retry is not enabled
	if err = hs.doSend(ff...); err == nil || hs.RetryCount <= 0 {
		return
	}

	// Loop over our tries
	for try := 1; try <= hs.RetryCount; try++ {
		// For sanity, we should handshake to get a new transaction id
		hs.Handshake()

		// Reset all the readers
		for _, f := range ff {
			if resetErr := f.Reset(); resetErr != nil {
				return resetErr
			}
		}

		if Debug {
			log.Println("Retrying send,", try, "err:", err)
		}

		if hs.OnRetry != nil {
			hs.OnRetry(ff, try, err) // Call preamble function
		}

		// do the work
		err = hs.doSend(ff...)

		if Debug {
			log.Println("Send came back with,", err)
		}

		if err == nil {
			break
		}

		// hold off, handshake, and retry
		time.Sleep(hs.RetryDelay)
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
	err       error

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
	w := &Writer{w: hw.w}
	n, err = w.Write(f)
	hw.Sent += n
	return
}

// Close the HTTPPostWriter and flush the data to the stream
func (hw *HTTPPostWriter) Close() (err error) {
	if hw.err != nil {
		return hw.err
	}

	hw.writeLock.Lock()
	defer hw.writeLock.Unlock()
	if hw.w == hw.pw {
		hw.w.Close()
		hw.w = nil
	} else {
		hw.w.Close()
		hw.w = nil
		hw.pw.Close()
		hw.pw = nil
	}

	if Debug {
		log.Println("closed channel, waiting for post reply")
	}
	hw.err = <-hw.clientErr
	if Debug {
		log.Println("replied!", hw.err, hw.Response)
	}

	return hw.err
}

// Terminate the HTTPPostWriter
func (hw *HTTPPostWriter) Terminate() {
	if mlw, ok := hw.w.(*maxLatencyWriter); ok {
		mlw.dst.Reset(nil)
	}
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

	if Debug {
		log.Printf("HTTP.Client: %#v\n", *hs.client)
	}

	r, w := io.Pipe()
	httpWriter = &HTTPPostWriter{
		Header:    make(http.Header),
		pw:        w,
		w:         w,
		hs:        hs,
		client:    hs.client,
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

	httpWriter = &HTTPPostWriter{
		Header:        make(http.Header),
		pw:            w,
		w:             mlw,
		hs:            hs,
		FlushInterval: 400 * time.Millisecond,
		client:        hs.client,
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

	if hs.TransactionID == "" { // Lazy init
		hs.Handshake()
	}

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
