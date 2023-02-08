package flowfile_test

import (
	"log"
	"net/http"
	"strings"

	"github.com/pschou/go-flowfile"
)

func ExampleNewHTTPTransaction_Forwarding() {
	txn, err := flowfile.NewHTTPTransaction("http://decimated:8080/contentListener", http.DefaultClient)
	if err != nil {
		log.Fatal(err)
	}

	var counter int
	myDecimator := flowfile.NewHTTPReceiver(func(s *flowfile.Scanner, r *http.Request) error {
		pw := txn.NewHTTPPostWriter()
		defer pw.Close()

		for s.Scan() {
			f, err := s.File()
			if err != nil {
				return err
			}

			counter++
			if counter%10 == 1 {
				_, err = pw.Write(f) // Forward only 1 of every 10 Files
				if err != nil {
					return err
				}
			}
		}
		return nil // Drop the rest
	})

	http.Handle("/contentDecimator", myDecimator) // Add the listener to a path
	http.ListenAndServe(":8080", nil)             // Start accepting connections
}

func ExampleNewHTTPTransaction() {
	// Create a new HTTPTransaction, used for sending batches of flowfiles
	hs, err := flowfile.NewHTTPTransaction("http://localhost:8080/contentListener", http.DefaultClient)
	if err != nil {
		log.Fatal(err)
	}

	dat := []byte("NiFiFF3\x00\x02\x00\x04path\x00\x02./\x00\bfilename\x00\tabcd-efgh\x00\x00\x00\x00\x00\x00\x00$this is a custom string for flowfile")
	var ff flowfile.File
	err = flowfile.Unmarshal(dat, &ff)

	err = hs.Send(&ff)
}

func ExampleSendConfig() {
	// Create a new HTTPTransaction, used for sending batches of flowfiles
	hs, err := flowfile.NewHTTPTransaction("http://localhost:8080/contentListener", http.DefaultClient)
	if err != nil {
		log.Fatal(err)
	}

	dat := []byte("NiFiFF3\x00\x02\x00\x04path\x00\x02./\x00\bfilename\x00\tabcd-efgh\x00\x00\x00\x00\x00\x00\x00$this is a custom string for flowfile")
	var ff flowfile.File
	err = flowfile.Unmarshal(dat, &ff)

	hp := hs.NewHTTPPostWriter()
	defer hp.Close()

	hp.Header.Set("X-Forwarded-For", "1.2.3.4:5678")
	_, err = hp.Write(&ff)
}

func ExampleNewHTTPFileReceiver() {
	ffReceiver := flowfile.NewHTTPFileReceiver(func(f *flowfile.File, r *http.Request) error {
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
	ffReceiver := flowfile.NewHTTPReceiver(func(fs *flowfile.Scanner, r *http.Request) error {
		// Loop over all the files in the post payload
		count := 0
		for fs.Scan() {
			count++
			f, err := fs.File()
			if err != nil {
				return err
			}
			log.Println("Got file", f.Attrs.Get("filename"))
			// do stuff with file
		}
		log.Println(count, "file(s) in POST payload")
		return nil
	})

	// Add this reciever to the path
	http.Handle("/contentListener", ffReceiver)

	// Start accepting files
	http.ListenAndServe(":8080", nil)
}

func ExampleHTTPPostWriter() {
	ff1 := flowfile.New(strings.NewReader("test1"), 5)
	ff2 := flowfile.New(strings.NewReader("test2"), 5)
	ht, err := flowfile.NewHTTPTransaction("http://localhost:8080/contentListener", http.DefaultClient)
	if err != nil {
		log.Fatal(err)
	}

	w := ht.NewHTTPPostWriter() // Create the POST to the NiFi endpoint
	w.Write(ff1)
	w.Write(ff2)
	err = w.Close() // Finalize the POST
}
