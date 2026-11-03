// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
)

type watchBufferSink struct {
	bytes.Buffer
	writes  int
	failure error
}

func (s *watchBufferSink) Write(p []byte) (int, error) {
	s.writes++
	if s.failure != nil {
		return 0, s.failure
	}
	return s.Buffer.Write(p)
}
func watchBatchBytes(events int) []byte {
	var data bytes.Buffer
	for i := 0; i < events; i++ {
		var header [4]byte
		binary.BigEndian.PutUint32(header[:], 4096)
		data.Write(header[:])
		data.Write(bytes.Repeat([]byte{byte(i)}, 4096))
	}
	return data.Bytes()
}
func writeWatchDataFrames(w io.Writer, payload []byte) error {
	f := NewFramer(w, nil)
	for len(payload) > 0 {
		n := len(payload)
		if n > 16384 {
			n = 16384
		}
		if err := f.WriteData(1, false, payload[:n]); err != nil {
			return err
		}
		payload = payload[n:]
	}
	return nil
}
func TestBufferedWriterWatchBatch(t *testing.T) {
	for _, events := range []int{1, 8, 32} {
		t.Run(fmt.Sprint(events), func(t *testing.T) {
			sink := &watchBufferSink{}
			w := newBufferedWriter(sink)
			payload := watchBatchBytes(events)
			if err := writeWatchDataFrames(w, payload); err != nil {
				t.Fatal(err)
			}
			if events <= 8 && sink.writes != 0 {
				t.Fatalf("batch flushed early: %d writes", sink.writes)
			}
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
			if events <= 8 && sink.writes != 1 {
				t.Fatalf("want one write, got %d", sink.writes)
			}
			var decoded bytes.Buffer
			f := NewFramer(nil, bytes.NewReader(sink.Bytes()))
			for {
				frame, err := f.ReadFrame()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				d, ok := frame.(*DataFrame)
				if !ok || d.StreamID != 1 || d.StreamEnded() {
					t.Fatalf("unexpected frame: %#v", frame)
				}
				decoded.Write(d.Data())
			}
			if !bytes.Equal(decoded.Bytes(), payload) {
				t.Fatal("DATA framing changed watch bytes or order")
			}
			before := sink.writes
			if err := w.Flush(); err != nil || sink.writes != before {
				t.Fatal("empty Flush wrote data")
			}
			t.Logf("events=%d watch_bytes=%d wire_bytes=%d underlying_writes=%d", events, len(payload), sink.Len(), sink.writes)
		})
	}
}
func TestBufferedWriterFlushErrorAndReuse(t *testing.T) {
	failure := errors.New("write failed")
	sink := &watchBufferSink{failure: failure}
	w := newBufferedWriter(sink)
	if _, err := w.Write([]byte("one")); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != failure {
		t.Fatalf("want write error, got %v", err)
	}
	if w.bw != nil {
		t.Fatal("failed buffer was not released")
	}
	sink.failure = nil
	if _, err := w.Write([]byte("two")); err != nil {
		t.Fatal(err)
	}
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	if sink.String() != "two" {
		t.Fatalf("stale bytes after reuse: %q", sink.String())
	}
}
func BenchmarkWatchConnectionBuffer(b *testing.B) {
	payload := watchBatchBytes(8)
	for _, size := range []int{4 << 10, 16 << 10, 32 << 10, 64 << 10} {
		b.Run(fmt.Sprintf("%dKiB", size>>10), func(b *testing.B) {
			sink := &watchBufferSink{}
			w := bufio.NewWriterSize(sink, size)
			b.SetBytes(int64(len(payload)))
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				sink.Buffer.Reset()
				if err := writeWatchDataFrames(w, payload); err != nil {
					b.Fatal(err)
				}
				if err := w.Flush(); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(sink.writes)/float64(b.N), "writes/batch")
		})
	}
}

// Block server writes after the HTTP/2 handshake, independent of OS TCP buffer
// sizes and connection buffering, so control-frame queue exhaustion is real.
type pausedWriteConn struct {
	net.Conn
	pause  chan struct{}
	closed chan struct{}
	once   sync.Once
}

func (c *pausedWriteConn) Write(p []byte) (int, error) {
	select {
	case <-c.pause:
		<-c.closed
		return 0, io.ErrClosedPipe
	default:
		return c.Conn.Write(p)
	}
}
func (c *pausedWriteConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

type pausedWriteListener struct {
	net.Listener
	accepted chan *pausedWriteConn
}

func (l *pausedWriteListener) Accept() (net.Conn, error) {
	c, err := l.Listener.Accept()
	if err != nil {
		return nil, err
	}
	p := &pausedWriteConn{Conn: c, pause: make(chan struct{}), closed: make(chan struct{})}
	l.accepted <- p
	return p, nil
}
