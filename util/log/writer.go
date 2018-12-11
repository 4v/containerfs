package log

import (
	"os"
	"bytes"
	"sync"
	"time"
	"io"
)

// AsyncFileWriter is an implementation of io.Writer with file batch and async persist support.
type asyncFileWriter struct {
	file      *os.File
	buffer    *bytes.Buffer
	mu        sync.Mutex
	closeC    chan struct{}
	closeOnce sync.Once
}

func (writer *asyncFileWriter) flushScheduler() {
	var (
		ticker *time.Ticker
	)
	ticker = time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			writer.flushToFile()
		case <-writer.closeC:
			ticker.Stop()
			return
		}
	}
}

func (writer *asyncFileWriter) Write(p []byte) (n int, err error) {
	writer.mu.Lock()
	writer.buffer.Write(p)
	writer.mu.Unlock()
	if writer.buffer.Len() > WriterBufferLenLimit {
		writer.flushToFile()
	}
	return
}

func (writer *asyncFileWriter) Close() (err error) {
	writer.mu.Lock()
	defer writer.mu.Unlock()
	writer.closeOnce.Do(func() {
		close(writer.closeC)
		writer.flushToFile()
		writer.file.Sync()
		writer.file.Close()
	})
	return
}

func (writer *asyncFileWriter) Flush() {
	writer.flushToFile()
}

func (writer *asyncFileWriter) flushToFile() {
	writer.mu.Lock()
	flushLength := writer.buffer.Len()
	if flushLength == 0 {
		writer.mu.Unlock()
		return
	}
	flushBytes := make([]byte, flushLength)
	copy(flushBytes, writer.buffer.Bytes())
	writer.buffer.Reset()
	writer.mu.Unlock()
	writer.file.Write(flushBytes[:flushLength])
	writer.file.Sync()
}

func NewAsyncFileWriter(out *os.File) io.WriteCloser {
	w := &asyncFileWriter{
		file:   out,
		buffer: bytes.NewBuffer(make([]byte, 0, WriterBufferInitSize)),
		closeC: make(chan struct{}),
	}
	go w.flushScheduler()
	return w
}
