package flowfile_test

import (
	"log"
	"net/http"

	"github.com/pschou/go-flowfile"
)

func ExampleNewHTTPTransaction() {
	// Create a new HTTPTransaction, used for sending batches of flowfiles
	hs, err := flowfile.NewHTTPTransaction("http://localhost:8080/contentListener", http.DefaultClient)
	if err != nil {
		log.Fatal(err)
	}

	dat := []byte("NiFiFF3\x00\x02\x00\x04path\x00\x02./\x00\bfilename\x00\tabcd-efgh\x00\x00\x00\x00\x00\x00\x00$this is a custom string for flowfile")
	ff, _ := flowfile.Unmarshal(dat)

	err = hs.Send(ff, nil)
}

func ExampleSendConfig() {
	// Create a new HTTPTransaction, used for sending batches of flowfiles
	hs, err := flowfile.NewHTTPTransaction("http://localhost:8080/contentListener", http.DefaultClient)
	if err != nil {
		log.Fatal(err)
	}

	dat := []byte("NiFiFF3\x00\x02\x00\x04path\x00\x02./\x00\bfilename\x00\tabcd-efgh\x00\x00\x00\x00\x00\x00\x00$this is a custom string for flowfile")
	ff, _ := flowfile.Unmarshal(dat)

	sendConfig := new(flowfile.SendConfig)
	sendConfig.SetHeader("X-Forwarded-For", "1.2.3.4:5678")
	err = hs.Send(ff, sendConfig)
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
