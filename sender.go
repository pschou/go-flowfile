package flowfile

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// The HTTP Sender will establish a NiFi handshake and ensure that the remote
// endpoint is listening and compatible with the current flow file format.
type HTTPSender struct {
	BytesSeen     uint64
	url           string
	client        *http.Client
	Server        string
	TransactionID string

	// Non-standard NiFi entities supported by this library
	MaxPartitionSize int    // Maximum partition size for partitioned file
	CheckSumType     string // What kind of CheckSum to use for sent files
	ErrorCorrection  float64
}

// Create the HTTP sender and verify that the remote side is listening.
func NewHTTPSender(url string, client *http.Client) (*HTTPSender, error) {
	req, err := http.NewRequest("HEAD", url, nil)
	if err != nil {
		return nil, err
	}

	txid := uuid.New().String()
	req.Header.Set("x-nifi-transaction-id", txid)
	req.Header.Set("User-Agent", UserAgent)
	res, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != 200 {
		return nil, fmt.Errorf("Unexpected status code %d", res.StatusCode)
	}

	// If the initial post was redirected, we'll want to stick with the final URL
	url = res.Request.URL.String()

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
			return nil, fmt.Errorf("Server does not support flowfile-v3")
		}
	}

	// Check for protocol version
	switch v := res.Header.Get("x-nifi-transfer-protocol-version"); v {
	case "3": // Add more versions after verifying support is there
	default:
		return nil, fmt.Errorf("Unknown NiFi TransferVersion %q", v)
	}

	// Parse out non-standard fields
	maxPartitionSize, _ := strconv.Atoi(res.Header.Get("x-ff-max-partition-size"))
	errorCorrection, _ := strconv.ParseFloat(res.Header.Get("x-ff-error-correction"), 64)

	return &HTTPSender{
		url:              url,
		client:           client,
		TransactionID:    txid,
		Server:           res.Header.Get("Server"),
		MaxPartitionSize: maxPartitionSize,
		CheckSumType:     "SHA256",
		ErrorCorrection:  errorCorrection,
	}, nil
}

type SendConfig struct {
	Header http.Header
}

// Send a flow file to the remote server and return any errors back.
// A nil return is a successful send.
func (hs *HTTPSender) Send(f *File, cfg *SendConfig) error {
	f.AddChecksum(hs.CheckSumType)
	r, w := io.Pipe()
	defer r.Close() // Make sure pipe is terminated
	req, err := http.NewRequest("POST", hs.url, r)
	if err != nil {
		return err
	}
	go func() {
		SendFiles(w, []*File{f})
		w.Close()
	}()

	// Set custom http headers
	if cfg != nil && cfg.Header != nil {
		for k, v := range cfg.Header {
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
	res, err := hs.client.Do(req)
	if err != nil {
		return err
	}
	if res.StatusCode != 200 {
		return fmt.Errorf("File did not send successfully")
	}
	return nil
}
