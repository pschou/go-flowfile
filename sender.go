package flowfile // import "github.com/pschou/go-flowfile"

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"

	"github.com/google/uuid"
)

// The HTTP Sender will establish a NiFi handshake and ensure that the remote
// endpoint is listening and compatible with the current flow file format.
type HTTPSession struct {
	BytesSeen     uint64
	url           string
	client        *http.Client
	Server        string
	TransactionID string

	// Non-standard NiFi entities supported by this library
	MaxPartitionSize int    // Maximum partition size for partitioned file
	CheckSumType     string // What kind of CheckSum to use for sent files
	ErrorCorrection  float64

	hold *bool
	//waitGroup sizedwaitgroup.SizedWaitGroup
	wait sync.RWMutex
}

// Create the HTTP sender and verify that the remote side is listening.
func NewHTTPSession(url string, client *http.Client) (*HTTPSession, error) {
	hs := &HTTPSession{
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

func (hs *HTTPSession) Handshake() error {
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
	if v := res.Header.Get("error-correction"); v != "" {
		errorCorrection, err := strconv.ParseFloat(v, 64)
		if err != nil {
			hs.ErrorCorrection = errorCorrection
		}
	}

	hs.TransactionID = txid
	hs.Server = res.Header.Get("Server")
	return nil
}

type SendConfig struct {
	header http.Header
}

func (c *SendConfig) GetHeader(key string) (value string) {
	if c.header == nil {
		c.header = make(http.Header)
	}
	return c.header.Get(key)
}
func (c *SendConfig) SetHeader(key, value string) {
	if c.header == nil {
		c.header = make(http.Header)
	}
	c.header.Set(key, value)
}

// Send a flow file to the remote server and return any errors back.
// A nil return for error is a successful send.
func (hs *HTTPSession) Send(f *File, cfg *SendConfig) (err error) {
	httpWriter := hs.NewHTTPWriter(cfg)
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

func (hs *HTTPSession) Close() (err error) {
	//hs.waitGroup.Wait()
	return
}

type HTTPWriter struct {
	hs        *HTTPSession
	w         *io.PipeWriter
	err       error
	clientErr error
	Response  *http.Response
	replyLock sync.Mutex
}

// Write a flow file to the remote server and return any errors back.  One
// cannot determine if there has been a successful send until the HTTPWriter is
// closed.  Then the Response.StatusCode will be set with the reply from the
// server.
func (hw *HTTPWriter) Write(f *File) error {
	if hw.clientErr != nil {
		return hw.clientErr
	}
	if f.Size > 0 && f.Attrs.Get("checksum-type") == "" {
		//if Debug {
		//	fmt.Println("  doing checksum")
		//}
		f.AddChecksum(hw.hs.CheckSumType)
	}
	//if Debug {
	//	fmt.Println("  write to stream")
	//}
	return writeTo(hw.w, f)
}

// Close the HTTPWriter and flush the data to the stream
func (hw *HTTPWriter) Close() (err error) {
	hw.w.Close()
	hw.replyLock.Lock()
	hw.replyLock.Unlock()
	return hw.clientErr
}

func (hs *HTTPSession) NewHTTPWriter(cfg *SendConfig) (httpWriter *HTTPWriter) {
	r, w := io.Pipe()
	httpWriter = &HTTPWriter{w: w, hs: hs}
	//hs.waitGroup.Add()

	httpWriter.replyLock.Lock()
	go func() {
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
	}()
	return
}
