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

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/apiserver/pkg/storage"
)

// ShardFilterConfig defines the shard identity for an apiserver operating as a shard.
type ShardFilterConfig struct {
	// ShardID is the 0-indexed ID of this shard.
	ShardID int
	// TotalShards is the total number of shards in the cluster topology.
	TotalShards int
	// Hasher calculates shard distribution.
	Hasher Hasher
}

// ShardFilter filters storage objects and events so an apiserver cache only holds its partition.
type ShardFilter struct {
	config  ShardFilterConfig
	keyFunc func(runtime.Object) (string, error)
}

// NewShardFilter constructs a ShardFilter.
func NewShardFilter(config ShardFilterConfig, keyFunc func(runtime.Object) (string, error)) (*ShardFilter, error) {
	if config.TotalShards <= 0 {
		return nil, fmt.Errorf("TotalShards must be > 0, got %d", config.TotalShards)
	}
	if config.ShardID < 0 || config.ShardID >= config.TotalShards {
		return nil, fmt.Errorf("ShardID %d is invalid for TotalShards %d", config.ShardID, config.TotalShards)
	}
	if config.Hasher == nil {
		config.Hasher = FNVHasher{}
	}
	return &ShardFilter{
		config:  config,
		keyFunc: keyFunc,
	}, nil
}

// MatchesKey checks if a storage key belongs to this shard.
func (sf *ShardFilter) MatchesKey(key string) bool {
	if sf.config.TotalShards <= 1 {
		return true
	}
	objKey := ExtractObjectKey(key)
	return sf.config.Hasher.HashKey(objKey, sf.config.TotalShards) == sf.config.ShardID
}

// MatchesObject checks if an object belongs to this shard.
func (sf *ShardFilter) MatchesObject(obj runtime.Object) (bool, error) {
	if sf.config.TotalShards <= 1 {
		return true, nil
	}
	key, err := sf.keyFunc(obj)
	if err != nil {
		return false, err
	}
	return sf.MatchesKey(key), nil
}

// FilterEvent returns true if the watch event belongs to this shard partition.
func (sf *ShardFilter) FilterEvent(event *watch.Event) (bool, error) {
	// Always allow Bookmarks through so watermark progress tracking stays live.
	if event.Type == watch.Bookmark {
		return true, nil
	}
	return sf.MatchesObject(event.Object)
}

// ShardedBackendWrapper wraps an underlying storage.Interface on a shard apiserver
// so that only objects belonging to this shard are ingested into its cache.
type ShardedBackendWrapper struct {
	storage.Interface
	filter *ShardFilter
}

// NewShardedBackendWrapper creates a wrapper around storage.Interface for a shard apiserver.
func NewShardedBackendWrapper(storageBackend storage.Interface, filter *ShardFilter) *ShardedBackendWrapper {
	return &ShardedBackendWrapper{
		Interface: storageBackend,
		filter:    filter,
	}
}
