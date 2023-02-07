package flowfile // import "github.com/pschou/go-flowfile"

import (
	"bufio"
	"fmt"
	"io"
	"log"
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
	BytesSeen     uint64
	url           string
	client        *http.Client
	Server        string
	TransactionID string

	// Non-standard NiFi entities supported by this library
	MaxPartitionSize int    // Maximum partition size for partitioned file
	CheckSumType     string // What kind of CheckSum to use for sent files

	hold *bool
	//waitGroup sizedwaitgroup.SizedWaitGroup
	wait sync.RWMutex
}

// Create the HTTP sender and verify that the remote side is listening.
func NewHTTPTransaction(url string, client *http.Client) (*HTTPTransaction, error) {
	hs := &HTTPTransaction{
		url:          url,
		client:       client,
		CheckSumType: "SHA256",
		//waitGroup:    sizedwaitgroup.New(4),
	}

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

	req, err := http.NewRequest("HEAD", hs.url, nil)
	if err != nil {
		return err
	}

	txid := uuid.New().String()
	req.Header.Set("x-nifi-transaction-id", txid)
	req.Header.Set("User-Agent", UserAgent)
	res, err := hs.client.Do(req)
	if err != nil {
		return err
	}

	if res.StatusCode != 200 {
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
		maxPartitionSize, err := strconv.Atoi(v)
		if err == nil {
			hs.MaxPartitionSize = maxPartitionSize
		} else {
			if Debug {
				log.Println("Unable to parse Max-Partition-Size", err)
			}
		}
	}

	hs.TransactionID, hs.Server = txid, res.Header.Get("Server")
	return nil
}

type SendConfig struct {
	header        http.Header
	FlushInterval time.Duration
}

// Get a header value which is configured to be sent
func (c *SendConfig) GetHeader(key string) (value string) {
	if c.header == nil {
		c.header = make(http.Header)
	}
	return c.header.Get(key)
}

// Set a header value which is configured to be sent
func (c *SendConfig) SetHeader(key, value string) {
	if c.header == nil {
		c.header = make(http.Header)
	}
	c.header.Set(key, value)
}

// Send a flow file to the remote server and return any errors back.
// A nil return for error is a successful send.
func (hs *HTTPTransaction) Send(f *File, cfg *SendConfig) (err error) {
	httpWriter := hs.NewHTTPPostWriter(cfg)
	defer func() {
		httpWriter.Close()
		if httpWriter.Response == nil {
			err = fmt.Errorf("File did not send, no response")
		} else if httpWriter.Response.StatusCode != 200 {
			err = fmt.Errorf("File did not send successfully, code %d", httpWriter.Response.StatusCode)
		}
	}()
	err = httpWriter.Write(f)
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
//   cfg := &flowfile.SendConfig{}  // Set the sent HTTP Headers with this
//   w := ht.NewHTTPPostWriter(cfg) // Create the POST to the NiFi endpoint
//   w.Write(ff1)
//   w.Write(ff2)
//   err = w.Close() // Finalize the POST
type HTTPPostWriter struct {
	hs        *HTTPTransaction
	w         io.WriteCloser
	err       error
	clientErr error
	Response  *http.Response
	replyLock sync.Mutex
}

// Write a flow file to the remote server and return any errors back.  One
// cannot determine if there has been a successful send until the HTTPPostWriter is
// closed.  Then the Response.StatusCode will be set with the reply from the
// server.
func (hw *HTTPPostWriter) Write(f *File) error {
	if hw.clientErr != nil {
		return hw.clientErr
	}
	if f.Size > 0 && f.Attrs.Get("checksum-type") == "" {
		f.AddChecksum(hw.hs.CheckSumType)
	}
	return writeTo(hw.w, f)
}

// Close the HTTPPostWriter and flush the data to the stream
func (hw *HTTPPostWriter) Close() (err error) {
	hw.w.Close()
	hw.replyLock.Lock()
	hw.replyLock.Unlock()
	return hw.clientErr
}

// NewHTTPPostWriter creates a POST to a NiFi listening endpoint and allows
// multiple files to be written to the endpoint at one time.  This reduces
// additional overhead (with fewer HTTP reponses) and decreases latency (by
// instead putting pressure on the TCP payload size).
//
// However, HTTPPostWriter increases the chances of failures as all the sent
// files will be marked as failed if the the HTTP POST is not a success.
func (hs *HTTPTransaction) NewHTTPPostWriter(cfg *SendConfig) (httpWriter *HTTPPostWriter) {
	r, w := io.Pipe()
	httpWriter = &HTTPPostWriter{w: w, hs: hs}
	httpWriter.replyLock.Lock()
	go doPost(hs, httpWriter, r, cfg)
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
func (hs *HTTPTransaction) NewHTTPBufferedPostWriter(cfg *SendConfig) (httpWriter *HTTPPostWriter) {
	r, w := io.Pipe()
	if cfg.FlushInterval == 0 {
		cfg.FlushInterval = 400 * time.Millisecond
	}
	mlw := &maxLatencyWriter{
		dst:     bufio.NewWriter(w),
		c:       w,
		latency: cfg.FlushInterval,
		done:    make(chan bool),
	}
	go mlw.flushLoop()

	httpWriter = &HTTPPostWriter{w: mlw, hs: hs}
	httpWriter.replyLock.Lock()
	go doPost(hs, httpWriter, r, cfg)
	return
}

func doPost(hs *HTTPTransaction, httpWriter *HTTPPostWriter, r io.ReadCloser, cfg *SendConfig) {
	hs.wait.RLock()
	defer hs.wait.RUnlock()

	req, _ := http.NewRequest("POST", hs.url, r)
	// We shouldn't get an error here as the session would have already
	// established the connection details.

	defer httpWriter.replyLock.Unlock()
	defer r.Close() // Make sure pipe is terminated
	// Set custom http headers
	if cfg != nil && cfg.header != nil {
		for k, v := range cfg.header {
			if len(v) > 0 {
				req.Header.Set(k, v[0])
			}
		}
	}

	req.Header.Set("Content-Type", "application/flowfile-v3")
	req.Header.Set("x-nifi-transfer-protocol-version", "3")
	req.Header.Set("x-nifi-transaction-id", hs.TransactionID)
	req.Header.Set("Transfer-Encoding", "chunked")
	req.Header.Set("User-Agent", UserAgent)
	httpWriter.Response, httpWriter.clientErr = hs.client.Do(req)
	if Debug {
		log.Println("set reponse", httpWriter.Response, httpWriter.clientErr)
	}
}
