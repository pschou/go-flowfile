package flowfile_test

import (
	"crypto/tls"
	"log"
	"net/http"
	"strings"

	"github.com/pschou/go-flowfile"
)

var tlsConfig *tls.Config

func ExampleNewHTTPTransaction_FilteredForward() {
	// Create a endpoint to send FlowFiles to:
	txn, err := flowfile.NewHTTPTransaction("http://target:8080/contentListener", nil)
	if err != nil {
		log.Fatal(err)
	}

	// Setup a receiver method to deal with incoming flowfiles
	myFilter := flowfile.NewHTTPFileReceiver(func(f *flowfile.File, w http.ResponseWriter, r *http.Request) error {
		if f.Attrs.Get("project") == "ProjectA" {
			return txn.Send(f) // Forward only ProjectA related FlowFiles
		}
		return nil // Drop the rest
	})
	http.Handle("/contentListener", myFilter) // Add the listener to a path
	http.ListenAndServe(":8080", nil)         // Start accepting connections
}

func ExampleNewHTTPTransaction_ForwardingWithCounter() {
	txn, err := flowfile.NewHTTPTransaction("http://decimated:8080/contentListener", tlsConfig)
	if err != nil {
		log.Fatal(err)
	}

	var counter int
	myDecimator := flowfile.NewHTTPReceiver(func(s *flowfile.Scanner, w http.ResponseWriter, r *http.Request) {
		pw := txn.NewHTTPPostWriter()
		defer pw.Close() // Ensure the POST is sent when the transaction finishes.

		for s.Scan() {
			f := s.File()
			if counter++; counter%10 == 1 { // Forward only 1 of every 10 Files
				if _, err = pw.Write(f); err != nil { // Oops, something unexpected bad happened
					w.WriteHeader(http.StatusInternalServerError) // Return an error
					pw.Terminate()
					return
				}
			}
		}
		if err := s.Err(); err != nil {
			log.Println("Error:", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK) // Drop the rest by claiming all is ok
	})

	http.Handle("/contentDecimator", myDecimator) // Add the listener to a path
	http.ListenAndServe(":8080", nil)             // Start accepting connections
}

func ExampleNewHTTPTransaction() {
	// Create a new HTTPTransaction, used for sending batches of flowfiles
	hs, err := flowfile.NewHTTPTransaction("http://localhost:8080/contentListener", tlsConfig)
	if err != nil {
		log.Fatal(err)
	}

	dat := []byte("NiFiFF3\x00\x02\x00\x04path\x00\x02./\x00\bfilename\x00\tabcd-efgh\x00\x00\x00\x00\x00\x00\x00$this is a custom string for flowfile")
	var ff flowfile.File
	err = ff.UnmarshalBinary(dat)

	err = hs.Send(&ff)
}

func ExampleHTTPPostWriter_sendWithCustomHeader() {
	// Create a new HTTPTransaction, used for sending batches of flowfiles
	hs, err := flowfile.NewHTTPTransaction("http://localhost:8080/contentListener", tlsConfig)
	if err != nil {
		log.Fatal(err)
	}

	dat := []byte("NiFiFF3\x00\x02\x00\x04path\x00\x02./\x00\bfilename\x00\tabcd-efgh\x00\x00\x00\x00\x00\x00\x00$this is a custom string for flowfile")
	var ff flowfile.File
	err = ff.UnmarshalBinary(dat)

	hp := hs.NewHTTPPostWriter()
	defer hp.Close()

	hp.Header.Set("X-Forwarded-For", "1.2.3.4:5678")
	_, err = hp.Write(&ff)
}

func ExampleNewHTTPFileReceiver() {
	ffReceiver := flowfile.NewHTTPFileReceiver(func(f *flowfile.File, w http.ResponseWriter, r *http.Request) error {
		log.Println("Got file", f.Attrs.Get("filename"))
		// do stuff with file
		return nil
	})

	// Add this reciever to the path
	http.Handle("/contentListener", ffReceiver)

	// Start accepting files
	http.ListenAndServe(":8080", nil)
}

func ExampleNewHTTPReceiver() {
	ffReceiver := flowfile.NewHTTPReceiver(func(fs *flowfile.Scanner, w http.ResponseWriter, r *http.Request) {
		// Loop over all the files in the post payload
		count := 0
		for fs.Scan() {
			count++
			f := fs.File()
			log.Println("Got file", f.Attrs.Get("filename"))
			// do stuff with file
		}

		if err := fs.Err(); err != nil {
			log.Println("Error:", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		log.Println(count, "file(s) in POST payload")
		w.WriteHeader(http.StatusOK)
	})

	http.Handle("/contentListener", ffReceiver) // Add this reciever to the path
	http.ListenAndServe(":8080", nil)           // Start accepting files
}

func ExampleHTTPPostWriter() {
	// Build two small files
	ff1 := flowfile.New(strings.NewReader("test1"), 5)
	ff2 := flowfile.New(strings.NewReader("test2"), 5)

	// Prepare an HTTP transaction
	ht, err := flowfile.NewHTTPTransaction("http://localhost:8080/contentListener", tlsConfig)
	if err != nil {
		log.Fatal(err)
	}

	// Post the files to the endpoint
	w := ht.NewHTTPPostWriter()
	w.Write(ff1)
	w.Write(ff2)
	err = w.Close() // Finalize the POST
}
