//go:build failpoints

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

package failpoint

import (
	"sync"
)

var (
	mu       sync.RWMutex
	registry = make(map[string]func(interface{}))
)

// Enabled denotes whether failpoint instrumentation is compiled into the binary.
const Enabled = true

// Inject calls the registered failpoint function for 'name' if it exists, passing 'arg'.
func Inject(name string, arg interface{}) {
	mu.RLock()
	f, ok := registry[name]
	mu.RUnlock()
	if ok && f != nil {
		f(arg)
	}
}

// Eval returns true if a failpoint is currently registered for 'name'.
func Eval(name string) bool {
	mu.RLock()
	_, ok := registry[name]
	mu.RUnlock()
	return ok
}

// Register saves a function into the registry under the given name.
func Register(name string, f func(interface{})) {
	mu.Lock()
	defer mu.Unlock()
	registry[name] = f
}

// Unregister removes a specific failpoint from the registry.
func Unregister(name string) {
	mu.Lock()
	defer mu.Unlock()
	delete(registry, name)
}
