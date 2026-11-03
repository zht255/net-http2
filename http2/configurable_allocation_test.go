// Copyright 2026 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package http2

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

var configurableConsumed, configurableRest FrameWriteRequest

func BenchmarkConfigurableSplit(b *testing.B) {
	for _, enabled := range []bool{false, true} {
		name := "off"
		if enabled {
			name = "on"
		}
		b.Run(name, func(b *testing.B) {
			st := &stream{id: 1, sc: &serverConn{srv: &Server{EnableDataFrameAllocationOptimization: enabled}, maxFrameSize: 16384}}
			wr := FrameWriteRequest{write: &writeData{1, make([]byte, 32768), true}, stream: st, done: make(chan error, 1)}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				st.flow.n = 32768
				a, r, n := wr.Consume(32768)
				if n != 2 {
					b.Fatal(n)
				}
				configurableConsumed, configurableRest = a, r
			}
		})
	}
}

func BenchmarkConfigurablePoolRetention(b *testing.B) {
	defer disableGoroutineTracking()()
	sc := &serverConn{srv: &Server{EnableWriteDataPoolCleanup: true}, wantWriteFrameCh: make(chan FrameWriteRequest, 1), doneServing: make(chan struct{})}
	st := &stream{id: 1}
	seen := make(chan *writeData, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		for wr := range sc.wantWriteFrameCh {
			seen <- wr.write.(*writeData)
			wr.replyToWriter(nil)
		}
	}()
	payload := make([]byte, 1<<20)
	retained := 0
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := sc.writeDataFromHandler(st, payload, false); err != nil {
			b.Fatal(err)
		}
		retained = len((<-seen).p)
	}
	b.StopTimer()
	close(sc.wantWriteFrameCh)
	<-finished
	b.ReportMetric(float64(retained), "retained-payload-B")
}

func TestConfigurableSplitOwnership(t *testing.T) {
	st := &stream{id: 1, sc: &serverConn{srv: &Server{EnableDataFrameAllocationOptimization: true}, maxFrameSize: 16}}
	st.flow.add(64)
	data := &writeData{1, bytes.Repeat([]byte{7}, 48), true}
	done := make(chan error, 1)
	original := FrameWriteRequest{write: data, stream: st, done: done}
	first, rest, n := original.Consume(48)
	if n != 2 || len(data.p) != 48 || !data.endStream || original.done != done {
		t.Fatal("original request mutated")
	}
	second, last, n := rest.Consume(48)
	if n != 2 || first.DataSize() != 16 || second.DataSize() != 16 || last.DataSize() != 16 {
		t.Fatal("bad split")
	}
	if first.done != nil || second.done != nil || last.done != done {
		t.Fatal("completion notification changed")
	}
	if first.write.(*writeData).endStream || second.write.(*writeData).endStream || !last.write.(*writeData).endStream {
		t.Fatal("END_STREAM changed")
	}
	if !bytes.Equal(first.write.(*writeData).p, bytes.Repeat([]byte{7}, 16)) {
		t.Fatal("first descriptor changed after later split")
	}
}

func TestConfigurableCompletedPayloadReleased(t *testing.T) {
	sc := &serverConn{srv: &Server{EnableWriteDataPoolCleanup: true}, wantWriteFrameCh: make(chan FrameWriteRequest, 1), doneServing: make(chan struct{})}
	st := &stream{id: 1}
	seen := make(chan *writeData, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		wr := <-sc.wantWriteFrameCh
		seen <- wr.write.(*writeData)
		wr.replyToWriter(nil)
	}()
	if err := sc.writeDataFromHandler(st, make([]byte, 1<<20), true); err != nil {
		t.Fatal(err)
	}
	arg := <-seen
	<-finished
	if arg.p != nil || arg.streamID != 0 || arg.endStream {
		t.Fatal("completed pooled argument retains handler state")
	}
}

func TestConfigurableOptionsTLS(t *testing.T) {
	for _, split := range []bool{false, true} {
		for _, cleanup := range []bool{false, true} {
			for _, size := range []int{4096, 65536} {
				t.Run(fmt.Sprintf("split=%v/cleanup=%v/buffer=%d", split, cleanup, size), func(t *testing.T) {
					payload := bytes.Repeat([]byte("0123456789abcdef"), 8192)
					srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.Write(payload) }))
					if err := ConfigureServer(srv.Config, &Server{WriteBufferSize: size, EnableDataFrameAllocationOptimization: split, EnableWriteDataPoolCleanup: cleanup}); err != nil {
						t.Fatal(err)
					}
					srv.TLS = srv.Config.TLSConfig
					srv.StartTLS()
					defer srv.Close()
					tr := &Transport{TLSClientConfig: &tls.Config{InsecureSkipVerify: true}}
					defer tr.CloseIdleConnections()
					client := &http.Client{Transport: tr, Timeout: 5 * time.Second}
					var wg sync.WaitGroup
					for i := 0; i < 4; i++ {
						wg.Add(1)
						go func() {
							defer wg.Done()
							for j := 0; j < 4; j++ {
								resp, err := client.Get(srv.URL)
								if err != nil {
									t.Error(err)
									return
								}
								body, err := io.ReadAll(resp.Body)
								resp.Body.Close()
								if err != nil || resp.ProtoMajor != 2 || !bytes.Equal(body, payload) {
									t.Errorf("invalid HTTP/2 response: %v, bytes=%d", err, len(body))
									return
								}
							}
						}()
					}
					wg.Wait()
				})
			}
		}
	}
}

func TestConfigurableCleanupOwnership(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, abort := range []bool{false, true} {
			sc := &serverConn{srv: &Server{EnableWriteDataPoolCleanup: enabled}, wantWriteFrameCh: make(chan FrameWriteRequest, 1), doneServing: make(chan struct{})}
			st := &stream{id: 1}
			result := make(chan error, 1)
			payload := bytes.Repeat([]byte{42}, 32768)
			go func() { result <- sc.writeDataFromHandler(st, payload, true) }()
			wr := <-sc.wantWriteFrameCh
			arg := wr.write.(*writeData)
			if abort {
				close(sc.doneServing)
			} else {
				wr.replyToWriter(nil)
			}
			err := <-result
			if abort && err != errClientDisconnected {
				t.Fatalf("abort: %v", err)
			}
			if !abort && err != nil {
				t.Fatal(err)
			}
			if enabled && !abort {
				if arg.p != nil {
					t.Fatal("completed payload retained")
				}
			} else if !bytes.Equal(arg.p, payload) {
				t.Fatal("payload cleared before ownership ended or with option off")
			}
		}
	}
}
