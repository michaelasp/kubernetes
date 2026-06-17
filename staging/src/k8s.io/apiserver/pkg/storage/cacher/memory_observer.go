package cacher

import (
	"context"
	"runtime"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

// MemoryObserver monitors the process memory usage and triggers degraded mode
// on registered cachers if it exceeds a certain threshold.
type MemoryObserver struct {
	mu      sync.Mutex
	cachers []*Cacher

	thresholdBytes uint64
	checkInterval  time.Duration
	isDegraded     bool
}

// NewMemoryObserver creates a new memory observer with a given threshold.
func NewMemoryObserver(thresholdBytes uint64, checkInterval time.Duration) *MemoryObserver {
	return &MemoryObserver{
		thresholdBytes: thresholdBytes,
		checkInterval:  checkInterval,
		cachers:        make([]*Cacher, 0),
	}
}

// Register adds a Cacher to be managed by this observer.
func (mo *MemoryObserver) Register(c *Cacher) {
	mo.mu.Lock()
	defer mo.mu.Unlock()
	mo.cachers = append(mo.cachers, c)
	// If already degraded, apply immediately to the new cacher.
	if mo.isDegraded {
		c.SetDegradedMode(true)
	}
}

// Run starts the memory observation loop.
func (mo *MemoryObserver) Run(ctx context.Context) {
	ticker := time.NewTicker(mo.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			mo.checkMemory()
		}
	}
}

func (mo *MemoryObserver) checkMemory() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// We use HeapAlloc as a proxy for memory pressure.
	// In a real implementation, we'd look at container cgroups (e.g. memory.usage_in_bytes)
	// or free system memory to detect true pressure.
	currentlyDegraded := m.HeapAlloc > mo.thresholdBytes

	mo.mu.Lock()
	defer mo.mu.Unlock()

	if currentlyDegraded != mo.isDegraded {
		mo.isDegraded = currentlyDegraded
		klog.Infof("MemoryObserver state changed: DegradedMode=%v (HeapAlloc=%d, Threshold=%d)",
			mo.isDegraded, m.HeapAlloc, mo.thresholdBytes)

		for _, c := range mo.cachers {
			c.SetDegradedMode(mo.isDegraded)
		}
	}
}
