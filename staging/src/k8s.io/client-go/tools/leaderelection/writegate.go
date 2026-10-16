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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"

	"k8s.io/client-go/transport"
)

// ErrWriteGateClosed is returned when a non-safe HTTP request is rejected or
// cancelled because the leader election write gate is closed.
var ErrWriteGateClosed = errors.New("write gate is closed")

// WriteGate returns a transport wrapper that blocks write requests whenever
// this elector is not leading.
func (le *LeaderElector) WriteGate() transport.WrapperFunc {
	return func(rt http.RoundTripper) http.RoundTripper {
		return &writeGateRoundTripper{
			le:       le,
			delegate: rt,
		}
	}
}

// Pausable represents a component (such as a workqueue or controller) that can
// pause and resume work when the leader election write gate closes and opens.
type Pausable interface {
	Pause()
	Resume()
}

// RegisterPausable registers a Pausable to be paused when the write gate closes
// and resumed when the write gate opens.
func (le *LeaderElector) RegisterPausable(p Pausable) {
	if p == nil {
		return
	}
	le.writeGate.registerPausable(p)
}

type writeGate struct {
	mu          sync.Mutex
	open        bool
	epochCtx    context.Context
	cancelEpoch context.CancelCauseFunc
	pausables   []Pausable
}

func (g *writeGate) openGate() {
	g.mu.Lock()
	if g.open {
		g.mu.Unlock()
		return
	}
	g.open = true
	g.epochCtx, g.cancelEpoch = context.WithCancelCause(context.Background())
	pausables := append([]Pausable(nil), g.pausables...)
	g.mu.Unlock()

	for _, p := range pausables {
		p.Resume()
	}
}

func (g *writeGate) closeGate() {
	g.mu.Lock()
	if !g.open {
		g.mu.Unlock()
		return
	}
	g.open = false
	if g.cancelEpoch != nil {
		g.cancelEpoch(ErrWriteGateClosed)
		g.cancelEpoch = nil
	}
	pausables := append([]Pausable(nil), g.pausables...)
	g.mu.Unlock()

	for _, p := range pausables {
		p.Pause()
	}
}

func (g *writeGate) registerPausable(p Pausable) {
	g.mu.Lock()
	g.pausables = append(g.pausables, p)
	open := g.open
	g.mu.Unlock()

	if !open {
		p.Pause()
	}
}

func (g *writeGate) acquireEpoch() (context.Context, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.open {
		return nil, false
	}
	return g.epochCtx, true
}

type writeGateRoundTripper struct {
	le       *LeaderElector
	delegate http.RoundTripper
}

func isSafeMethod(method string) bool {
	switch method {
	case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
		return true
	default:
		return false
	}
}

func (rt *writeGateRoundTripper) recordRejection() {
	if rt.le != nil && rt.le.metrics != nil {
		rt.le.metrics.writeGateRejection(rt.le.config.Name)
	}
}

func (rt *writeGateRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if isSafeMethod(req.Method) {
		return rt.delegate.RoundTrip(req)
	}

	epochCtx, open := rt.le.writeGate.acquireEpoch()
	if !open {
		rt.recordRejection()
		return nil, ErrWriteGateClosed
	}

	reqCtx, cancel := context.WithCancelCause(req.Context())
	stop := context.AfterFunc(epochCtx, func() {
		cancel(ErrWriteGateClosed)
	})

	resp, err := rt.delegate.RoundTrip(req.WithContext(reqCtx))
	if !stop() {
		// The gate closed while the request was in flight (or raced with RoundTrip completion).
		if err != nil {
			cancel(nil)
			rt.recordRejection()
			if errors.Is(err, ErrWriteGateClosed) {
				return nil, err
			}
			return nil, fmt.Errorf("%w: %w", ErrWriteGateClosed, err)
		}
	}
	if err != nil {
		cancel(nil)
		return nil, err
	}
	if resp != nil && resp.Body != nil {
		resp.Body = &cancelOnCloseReadCloser{
			ReadCloser: resp.Body,
			cancel:     func() { cancel(nil) },
		}
	} else {
		cancel(nil)
	}
	return resp, nil
}

func (rt *writeGateRoundTripper) WrappedRoundTripper() http.RoundTripper {
	return rt.delegate
}

type cancelOnCloseReadCloser struct {
	io.ReadCloser
	once   sync.Once
	cancel func()
}

func (c *cancelOnCloseReadCloser) Close() error {
	defer c.once.Do(c.cancel)
	return c.ReadCloser.Close()
}
