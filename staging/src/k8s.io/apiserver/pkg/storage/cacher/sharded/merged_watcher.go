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

package sharded

import (
	"container/heap"
	"context"
	"sync"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
)

// MergedWatcher merges N watch streams into a single monotonically ordered stream.
type MergedWatcher struct {
	ctx        context.Context
	cancel     context.CancelFunc
	resultChan chan watch.Event
	watchers   []watch.Interface
	versioner  storage.Versioner
	numShards  int

	lock                 sync.Mutex
	shardRVs             []uint64
	initialEventsDone    []bool
	allInitialEventsDone bool
	hasInitialEvents     bool
	eventQueue           eventMinHeap
	stopped              bool
}

// NewMergedWatcher constructs and starts a MergedWatcher over N shard watchers.
func NewMergedWatcher(ctx context.Context, watchers []watch.Interface, versioner storage.Versioner, sendInitialEvents bool, initialRV uint64) *MergedWatcher {
	cCtx, cancel := context.WithCancel(ctx)
	shardRVs := make([]uint64, len(watchers))
	for i := range shardRVs {
		shardRVs[i] = initialRV
	}
	mw := &MergedWatcher{
		ctx:                  cCtx,
		cancel:               cancel,
		resultChan:           make(chan watch.Event, 1000),
		watchers:             watchers,
		versioner:            versioner,
		numShards:            len(watchers),
		shardRVs:             shardRVs,
		initialEventsDone:    make([]bool, len(watchers)),
		hasInitialEvents:     sendInitialEvents,
		allInitialEventsDone: !sendInitialEvents,
	}
	heap.Init(&mw.eventQueue)
	go mw.run()
	return mw
}

// Stop terminates all shard watchers and closes the merged stream.
func (mw *MergedWatcher) Stop() {
	mw.lock.Lock()
	if mw.stopped {
		mw.lock.Unlock()
		return
	}
	mw.stopped = true
	mw.cancel()
	for _, w := range mw.watchers {
		w.Stop()
	}
	mw.lock.Unlock()
}

// ResultChan returns the receive-only channel for ordered watch events.
func (mw *MergedWatcher) ResultChan() <-chan watch.Event {
	return mw.resultChan
}

func (mw *MergedWatcher) run() {
	defer close(mw.resultChan)
	var wg sync.WaitGroup

	for i, w := range mw.watchers {
		wg.Add(1)
		go func(shardIdx int, watcher watch.Interface) {
			defer wg.Done()
			for {
				select {
				case <-mw.ctx.Done():
					return
				case event, ok := <-watcher.ResultChan():
					if !ok {
						return
					}
					if err := mw.handleEvent(shardIdx, event); err != nil {
						return
					}
				}
			}
		}(i, w)
	}

	wg.Wait()
}

func isInitialEventsEndBookmark(event watch.Event) bool {
	if event.Type != watch.Bookmark {
		return false
	}
	if acc, err := meta.Accessor(event.Object); err == nil {
		annotations := acc.GetAnnotations()
		if annotations != nil && annotations[metav1.InitialEventsAnnotationKey] == "true" {
			return true
		}
	}
	return false
}

func (mw *MergedWatcher) handleEvent(shardIdx int, event watch.Event) error {
	mw.lock.Lock()
	defer mw.lock.Unlock()

	if mw.stopped {
		return nil
	}

	rv, err := mw.versioner.ObjectResourceVersion(event.Object)
	if err != nil {
		// If resource version cannot be parsed (e.g. error event), emit immediately.
		select {
		case mw.resultChan <- event:
		case <-mw.ctx.Done():
		}
		return nil
	}

	// Advance shard watermark
	if rv > mw.shardRVs[shardIdx] {
		mw.shardRVs[shardIdx] = rv
	}

	// Handle initial events phase for streaming watchlist
	if mw.hasInitialEvents && !mw.allInitialEventsDone {
		if isInitialEventsEndBookmark(event) {
			mw.initialEventsDone[shardIdx] = true
			if mw.checkAllInitialEventsDoneLocked() {
				// All shards have finished initial events.
				mw.allInitialEventsDone = true

				// Create and emit the single unified InitialEventsEnd bookmark.
				minRV := mw.getMinWatermarkLocked()
				bookmarkObj := event.Object.DeepCopyObject()
				if err := mw.versioner.UpdateObject(bookmarkObj, minRV); err == nil {
					unifiedBookmark := watch.Event{
						Type:   watch.Bookmark,
						Object: bookmarkObj,
					}
					select {
					case mw.resultChan <- unifiedBookmark:
					case <-mw.ctx.Done():
						return nil
					}
				}

				// Flush any live events that were queued during initial sync
				mw.flushQueueLocked()
			}
			return nil
		}

		if !mw.initialEventsDone[shardIdx] {
			// Stream initial ADDED events directly without stalling.
			select {
			case mw.resultChan <- event:
			case <-mw.ctx.Done():
				return mw.ctx.Err()
			}
			return nil
		}
	}

	// Live event processing: Queue non-bookmark events
	if event.Type != watch.Bookmark {
		heap.Push(&mw.eventQueue, rvEvent{
			event:    event,
			rv:       rv,
			shardIdx: shardIdx,
		})
	}

	if mw.allInitialEventsDone {
		mw.flushQueueLocked()
	}

	return nil
}

func (mw *MergedWatcher) checkAllInitialEventsDoneLocked() bool {
	for _, done := range mw.initialEventsDone {
		if !done {
			return false
		}
	}
	return true
}

func (mw *MergedWatcher) getMinWatermarkLocked() uint64 {
	if len(mw.shardRVs) == 0 {
		return 0
	}
	min := mw.shardRVs[0]
	for _, rv := range mw.shardRVs[1:] {
		if rv < min {
			min = rv
		}
	}
	return min
}

func (mw *MergedWatcher) flushQueueLocked() {
	minWatermark := mw.getMinWatermarkLocked()

	for mw.eventQueue.Len() > 0 && mw.eventQueue[0].rv <= minWatermark {
		next := heap.Pop(&mw.eventQueue).(rvEvent)
		select {
		case mw.resultChan <- next.event:
		case <-mw.ctx.Done():
			return
		}
	}
}
