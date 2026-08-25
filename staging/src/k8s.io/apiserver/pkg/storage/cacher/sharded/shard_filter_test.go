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
	"fmt"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/apis/example"
)

func podKeyFunc(obj runtime.Object) (string, error) {
	pod, ok := obj.(*example.Pod)
	if !ok {
		return "", fmt.Errorf("unexpected type %T", obj)
	}
	return fmt.Sprintf("/registry/pods/%s/%s", pod.Namespace, pod.Name), nil
}

func TestShardFilterPartitioning(t *testing.T) {
	totalShards := 4
	hasher := FNVHasher{}

	filters := make([]*ShardFilter, totalShards)
	for i := 0; i < totalShards; i++ {
		f, err := NewShardFilter(ShardFilterConfig{
			ShardID:     i,
			TotalShards: totalShards,
			Hasher:      hasher,
		}, podKeyFunc)
		if err != nil {
			t.Fatalf("failed to create ShardFilter %d: %v", i, err)
		}
		filters[i] = f
	}

	// Test 100 pods distributed across shards
	totalMatches := 0
	countsPerShard := make([]int, totalShards)

	for i := 0; i < 100; i++ {
		pod := &example.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      fmt.Sprintf("test-pod-%d", i),
			},
		}

		matchedShards := 0
		for shardID, f := range filters {
			matched, err := f.MatchesObject(pod)
			if err != nil {
				t.Fatalf("MatchesObject error: %v", err)
			}
			if matched {
				matchedShards++
				countsPerShard[shardID]++
			}
		}

		if matchedShards != 1 {
			t.Fatalf("pod %s matched %d shards, expected exactly 1", pod.Name, matchedShards)
		}
		totalMatches++
	}

	if totalMatches != 100 {
		t.Fatalf("expected 100 total matches, got %d", totalMatches)
	}

	// Verify reasonable distribution across shards
	for shardID, count := range countsPerShard {
		if count == 0 {
			t.Fatalf("shard %d received 0 objects", shardID)
		}
	}
}

func TestShardFilterBookmarks(t *testing.T) {
	f, err := NewShardFilter(ShardFilterConfig{
		ShardID:     1,
		TotalShards: 4,
	}, podKeyFunc)
	if err != nil {
		t.Fatalf("failed to create filter: %v", err)
	}

	bookmarkEvent := &watch.Event{
		Type:   watch.Bookmark,
		Object: &example.Pod{ObjectMeta: metav1.ObjectMeta{ResourceVersion: "100"}},
	}

	// Bookmarks must always pass through
	passed, err := f.FilterEvent(bookmarkEvent)
	if err != nil || !passed {
		t.Fatalf("expected Bookmark to pass through, got passed=%v, err=%v", passed, err)
	}
}
