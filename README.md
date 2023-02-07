# go-flowfile

flowfile is a GoLang module providing a set of tools to interact with NiFi
FlowFiles at a low level.  It's been finely tuned to handle the streaming
context best as memory and disk often have limitations.

This module was built to be both simple to use and extremely low level set
of tools to work with FlowFiles at wire speed.  Here is an example of a
basic filtering and forwarding method:

```golang
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

  var counter int
  myDecimator:= flowfile.NewHTTPFileReceiver(func(f *flowfile.File, r *http.Request) error {
    counter++
    if counter%10 == 1 {
      return txn.Send(f)  // Forward only 1 of every 10 Files
    }
    return nil            // Drop the rest
  })

  http.Handle("/contentListener", myFilter)  // Add the listener to a path
  http.Handle("/contentDecimator", myDecimator)  // Add the listener to a path
  http.ListenAndServe(":8080", nil)          // Start accepting connections
```

Note: The logic here starts at the first packet in the stream, by the time a
decision is made the streams are able to be connected together to avoid all
local caches.

The complexity of the decision logic can be as complex or as simple as one
desires and consume on one or more ports / listening paths, and send to as
many upstream servers as desired with concurrency.

# About FlowFiles

FlowFiles are at the heart of Apache NiFi and its flow-based design. A
FlowFile is a data record, which consists of a pointer to its content
(payload) and attributes to support the content, that is associated with one
or more provenance events. The attributes are key/value pairs that act as
the metadata for the FlowFile, such as the FlowFile filename. The content is
the actual data or the payload of the file.

For more documentation go to https://pkg.go.dev/github.com/pschou/go-flowfile .
