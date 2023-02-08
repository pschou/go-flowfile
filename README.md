# go-flowfile

flowfile is a GoLang module providing a set of tools to interact with NiFi
FlowFiles at a low level.  It's been finely tuned to handle the streaming
context best, as memory and disk often have limitations.

This module was built to be both simple-to-use and operate at a low level (at
the bytes) to work with FlowFiles at wire speed.  Here is an example of how a basic
filtering and forwarding method would be written:

```golang
func main() {
  // Create a endpoint to send FlowFiles to:
  txn, err := flowfile.NewHTTPTransaction("http://target:8080/contentListener", http.DefaultClient)
  if err != nil {
    log.Fatal(err)
  }

  // Setup a receiver method to deal with incoming flowfiles
  myFilter := flowfile.NewHTTPFileReceiver(func(f *flowfile.File, r *http.Request) error {
    if f.Attrs.Get("project") == "ProjectA" {
      return txn.Send(f)  // Forward only ProjectA related FlowFiles
    }
    return nil            // Drop the rest
  })
  http.Handle("/contentListener", myFilter)  // Add the listener to a path
  http.ListenAndServe(":8080", nil)          // Start accepting connections
}
```

Another slightly more complex program example of building a NiFi routing
program, this time it forwards 1 of every 10 FlowFiles while keeping the
bundles together in one POST:

```golang
func main() {
  txn, err := flowfile.NewHTTPTransaction("http://decimated:8080/contentListener", http.DefaultClient)
  if err != nil {
    log.Fatal(err)
  }

  var counter int
  myDecimator := flowfile.NewHTTPReceiver(func(s *flowfile.Scanner, r *http.Request) error {
    pw := txn.NewHTTPBufferedPostWriter()  // Create a new POST downstream
    defer pw.Close()               // Close the POST at function return

    for s.Scan() {                 // Read the next file off the stream
      f, err := s.File()
      if err != nil {
        return err
      }

      counter++
      if counter%10 == 1 {
        _, err = pw.Write(f)       // Forward only 1 of every 10 Files
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
```

More examples can be found: https://pkg.go.dev/github.com/pschou/go-flowfile#pkg-examples

Early logic is key!  When an incoming FlowFile is presented to the program,
what is presented are the attributes often seen in the first packet in the
stream, so by the time a decision is made on what to do with the FlowFile,
the destination and incoming streams can be connected together to avoid all
local caches and enable "fast-forwarding" of the original packets.

The complexity of the decision logic can be as complex or as simple as one
desires.  One can consume on one or more ports/listening paths and send to as
many downstream servers as desired with concurrency.

For more documentation, go to https://pkg.go.dev/github.com/pschou/go-flowfile .
