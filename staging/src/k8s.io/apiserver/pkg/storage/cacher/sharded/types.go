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
	"hash/fnv"
	"strings"

	"k8s.io/apimachinery/pkg/watch"
)

// Hasher defines an interface for mapping storage keys to shard indexes.
type Hasher interface {
	HashKey(key string, numShards int) int
}

// FNVHasher implements Hasher using 32-bit FNV-1a.
type FNVHasher struct{}

func (h FNVHasher) HashKey(key string, numShards int) int {
	if numShards <= 1 {
		return 0
	}
	f := fnv.New32a()
	f.Write([]byte(key))
	return int(f.Sum32() % uint32(numShards))
}

// ExtractObjectKey extracts the object key suitable for hashing from a storage path.
func ExtractObjectKey(key string) string {
	parts := strings.Split(strings.Trim(key, "/"), "/")
	if len(parts) >= 3 {
		// e.g. registry/pods/namespace/name or pods/namespace/name -> namespace/name
		return parts[len(parts)-2] + "/" + parts[len(parts)-1]
	}
	if len(parts) == 2 {
		// e.g. nodes/node-1 or registry/nodes/node-1 -> node-1
		return parts[1]
	}
	return strings.Trim(key, "/")
}

// IsExactObjectKey returns true if the key refers to a specific object rather than a collection prefix.
func IsExactObjectKey(key string) bool {
	parts := strings.Split(strings.Trim(key, "/"), "/")
	return len(parts) >= 3
}

// rvEvent represents a watch event tagged with its parsed ResourceVersion and originating shard.
type rvEvent struct {
	event     watch.Event
	rv        uint64
	shardIdx  int
	isInitial bool
}

// eventMinHeap implements heap.Interface for ordering rvEvent by ResourceVersion.
type eventMinHeap []rvEvent

func (h eventMinHeap) Len() int           { return len(h) }
func (h eventMinHeap) Less(i, j int) bool { return h[i].rv < h[j].rv }
func (h eventMinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *eventMinHeap) Push(x any)        { *h = append(*h, x.(rvEvent)) }
func (h *eventMinHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
