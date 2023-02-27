package flowfile

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

func (f Metrics) String(keyValuePairs ...string) string {
	var lbl, lblAdd string
	if len(keyValuePairs) > 1 {
		for i := 1; i < len(keyValuePairs); i += 2 {
			lblAdd += "," + fmt.Sprintf("%s=%q", keyValuePairs[i-1], keyValuePairs[i])
		}
		lbl = "{" + lblAdd[1:] + "}"
	}
	w := &strings.Builder{}
	tm := time.Now().UnixMilli()
	fmt.Fprintf(w, "flowfiles_started%s %d %d\n",
		lbl, f.metricsInitTime.UnixMilli(), tm)
	fmt.Fprintf(w, "flowfiles_transfered_bytes_sum%s %d %d\n",
		lbl, f.MetricsFlowFileTransferredSum, tm)
	fmt.Fprintf(w, "flowfiles_transfered_bytes_count%s %d %d\n",
		lbl, f.MetricsFlowFileTransferredCount, tm)
	fmt.Fprintf(w, "flowfiles_threads_active%s %d %d\n",
		lbl, f.MetricsThreadsActive, tm)
	fmt.Fprintf(w, "flowfiles_threads_terminated%s %d %d\n",
		lbl, f.MetricsThreadsTerminated, tm)
	fmt.Fprintf(w, "flowfiles_threads_queued%s %d %d\n",
		lbl, f.MetricsThreadsQueued, tm)
	var bk string
	for i, v := range f.MetricsFlowFileTransferredBucketValues {
		if i < len(f.MetricsFlowFileTransferredBuckets) {
			bk = fmt.Sprintf("%d", f.MetricsFlowFileTransferredBuckets[i])
		} else {
			bk = "+Inf"
		}
		fmt.Fprintf(w, "flowfiles_transfered_bytes_bucket{le=%q%s} %d %d\n", bk, lblAdd, v, tm)
	}
	return w.String()
}

func (hr *HTTPReceiver) MetricsHandler() http.Handler {
	return &Metrics{hr: hr}
}

func NewMetrics() *Metrics {
	return &Metrics{
		MetricsFlowFileTransferredBuckets: []int64{
			1e2, 2.5e2, 1e3,
			2.5e3, 1e4, 2.5e4, 1e5,
			2.5e5, 1e6, 2.5e6, 1e7,
			2.5e7, 1e8, 2.5e8, 1e9},
		MetricsFlowFileTransferredBucketValues: make([]int64, 16),
		metricsInitTime:                        time.Now(),
	}
}

type Metrics struct {
	hr *HTTPReceiver

	// Custom buckets can be defined by setting new buckets before ingesting data
	// Note the BucketValues is always N+1 sized, as the last is overflow
	MetricsFlowFileTransferredBuckets      []int64
	MetricsFlowFileTransferredBucketValues []int64
	MetricsFlowFileTransferredSum          int64
	MetricsFlowFileTransferredCount        int64

	//MetricsFlowFileReceivedSum   *int64
	//MetricsFlowFileReceivedCount *int64
	MetricsThreadsActive     int64
	MetricsThreadsTerminated int64
	MetricsThreadsQueued     int64
	metricsInitTime          time.Time
}

func (m Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if m.hr != nil {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(m.hr.Metrics.String()))
	}
}

func (f *Metrics) BucketCounter(size int64) {
	idx := 0
	for ; idx < len(f.MetricsFlowFileTransferredBuckets) &&
		f.MetricsFlowFileTransferredBuckets[idx] <= size; idx++ {
	}
	//if Debug {
	//  fmt.Println("bucket size", size, idx, "in", f.MetricsFlowFileTransferredBuckets)
	//}
	f.MetricsFlowFileTransferredBucketValues[idx] += 1
	f.MetricsFlowFileTransferredSum += size
	f.MetricsFlowFileTransferredCount += 1
}
