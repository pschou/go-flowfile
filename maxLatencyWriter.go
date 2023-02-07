package flowfile

import (
	"bufio"
	"io"
	"sync"
	"time"
)

type maxLatencyWriter struct {
	dst     *bufio.Writer
	c       io.Closer
	latency time.Duration

	mu   sync.Mutex // protects Write + Flush
	done chan bool
}

func (m *maxLatencyWriter) Write(p []byte) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.dst.Write(p)
}

func (m *maxLatencyWriter) flushLoop() {
	t := time.NewTicker(m.latency)
	defer t.Stop()
	for {
		select {
		case <-m.done:
			m.mu.Lock()
			m.dst.Flush()
			m.mu.Unlock()
			return
		case <-t.C:
			m.mu.Lock()
			m.dst.Flush()
			m.mu.Unlock()
		}
	}
}

func (m *maxLatencyWriter) Close() error { m.done <- true; return m.c.Close() }
