/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package leaderelection

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}

type fakeLeaderMetric struct {
	rejections atomic.Int32
}

func (f *fakeLeaderMetric) On(string)                {}
func (f *fakeLeaderMetric) Off(string)               {}
func (f *fakeLeaderMetric) SlowpathExercised(string) {}
func (f *fakeLeaderMetric) WriteGateRejection(string) {
	f.rejections.Add(1)
}

func TestWriteGateMethodsAndStates(t *testing.T) {
	safeMethods := []string{
		http.MethodGet,
		http.MethodHead,
		http.MethodOptions,
		http.MethodTrace,
	}
	writeMethods := []string{
		http.MethodPost,
		http.MethodPut,
		http.MethodPatch,
		http.MethodDelete,
		http.MethodConnect,
		"UNKNOWN_METHOD",
	}

	metric := &fakeLeaderMetric{}
	le := &LeaderElector{
		config: LeaderElectionConfig{Name: "test-lease"},
		metrics: &defaultLeaderMetrics{
			leader: metric,
		},
	}

	var delegateCalls atomic.Int32
	rt := le.WriteGate()(roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		delegateCalls.Add(1)
		return &http.Response{
			StatusCode: http.StatusOK,
			Body:       io.NopCloser(strings.NewReader("ok")),
		}, nil
	}))

	// 1. Initially closed: safe methods pass, write methods are rejected.
	for _, method := range safeMethods {
		req, err := http.NewRequest(method, "https://127.0.0.1/test", nil)
		if err != nil {
			t.Fatalf("NewRequest(%s): %v", method, err)
		}
		resp, err := rt.RoundTrip(req)
		if err != nil {
			t.Fatalf("expected safe method %s to pass while closed, got error: %v", method, err)
		}
		_ = resp.Body.Close()
	}
	if got := delegateCalls.Load(); got != int32(len(safeMethods)) {
		t.Fatalf("expected %d delegate calls for safe methods, got %d", len(safeMethods), got)
	}

	for _, method := range writeMethods {
		req, err := http.NewRequest(method, "https://127.0.0.1/test", nil)
		if err != nil {
			t.Fatalf("NewRequest(%s): %v", method, err)
		}
		_, err = rt.RoundTrip(req)
		if !errors.Is(err, ErrWriteGateClosed) {
			t.Fatalf("expected write method %s while closed to fail with ErrWriteGateClosed, got: %v", method, err)
		}
	}
	if got := metric.rejections.Load(); got != int32(len(writeMethods)) {
		t.Fatalf("expected %d write gate rejections recorded, got %d", len(writeMethods), got)
	}

	// 2. Open gate: both safe and write methods pass.
	le.writeGate.openGate()
	for _, method := range append(append([]string{}, safeMethods...), writeMethods...) {
		req, err := http.NewRequest(method, "https://127.0.0.1/test", nil)
		if err != nil {
			t.Fatalf("NewRequest(%s): %v", method, err)
		}
		resp, err := rt.RoundTrip(req)
		if err != nil {
			t.Fatalf("expected method %s to pass while open, got error: %v", method, err)
		}
		_ = resp.Body.Close()
	}

	// 3. Close again and reopen: verify transitions.
	le.writeGate.closeGate()
	postReq, err := http.NewRequest(http.MethodPost, "https://127.0.0.1/test", nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := rt.RoundTrip(postReq); !errors.Is(err, ErrWriteGateClosed) {
		t.Fatalf("expected POST to be rejected after close, got: %v", err)
	}

	le.writeGate.openGate()
	resp, err := rt.RoundTrip(postReq)
	if err != nil {
		t.Fatalf("expected POST to succeed after reopen, got: %v", err)
	}
	_ = resp.Body.Close()
}

func TestWriteGateCancelsInFlightWritesAndPreservesCompletedWrites(t *testing.T) {
	le := &LeaderElector{}
	le.writeGate.openGate()

	inFlightStarted := make(chan struct{})
	var startOnce sync.Once

	rt := le.WriteGate()(roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/inflight" {
			startOnce.Do(func() { close(inFlightStarted) })
			<-req.Context().Done()
			return nil, req.Context().Err()
		}
		// Completed write returns a body whose reads check req.Context().Err().
		ctx := req.Context()
		return &http.Response{
			StatusCode: http.StatusOK,
			Body: io.NopCloser(readFunc(func(p []byte) (int, error) {
				if err := ctx.Err(); err != nil {
					return 0, err
				}
				copy(p, "completed")
				return len("completed"), io.EOF
			})),
		}, nil
	}))

	// 1. Complete a write BEFORE the gate closes.
	completedReq, err := http.NewRequest(http.MethodPost, "https://127.0.0.1/completed", nil)
	if err != nil {
		t.Fatal(err)
	}
	completedResp, err := rt.RoundTrip(completedReq)
	if err != nil {
		t.Fatalf("unexpected error on completed write: %v", err)
	}
	defer completedResp.Body.Close()

	// 2. Start an in-flight write and close the gate while it is in flight.
	inFlightErrCh := make(chan error, 1)
	go func() {
		req, reqErr := http.NewRequest(http.MethodPut, "https://127.0.0.1/inflight", nil)
		if reqErr != nil {
			inFlightErrCh <- reqErr
			return
		}
		_, rtErr := rt.RoundTrip(req)
		inFlightErrCh <- rtErr
	}()

	<-inFlightStarted
	le.writeGate.closeGate()

	inFlightErr := <-inFlightErrCh
	if !errors.Is(inFlightErr, ErrWriteGateClosed) {
		t.Fatalf("expected in-flight write to fail with ErrWriteGateClosed on close, got: %v", inFlightErr)
	}

	// 3. Verify the write that completed before close is unaffected (its response body can still be read).
	bodyBytes, err := io.ReadAll(completedResp.Body)
	if err != nil {
		t.Fatalf("expected completed write response body to remain readable after close, got error: %v", err)
	}
	if string(bodyBytes) != "completed" {
		t.Fatalf("unexpected body %q", string(bodyBytes))
	}
}

type readFunc func([]byte) (int, error)

func (f readFunc) Read(p []byte) (int, error) {
	return f(p)
}
