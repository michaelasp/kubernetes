/*
Copyright 2024 The Kubernetes Authors.

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

package testing

import (
	"fmt"
	"strings"
	"sync"
	"testing"

	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/version"

	clientfeatures "k8s.io/client-go/features"
)

var (
	featuresOverridden     sync.Once
	overriddenFeaturesLock sync.Mutex
	overriddenFeatures     map[clientfeatures.Feature]string
	featureGates           *testFeatureGate
)

func init() {
	overriddenFeatures = map[clientfeatures.Feature]string{}
}

type featureGatesSetter interface {
	clientfeatures.Gates

	Set(clientfeatures.Feature, bool) error
}

// SetFeatureDuringTest sets the specified feature to the specified value for the duration of the test.
//
// Example use:
//
//	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, true)
func SetFeatureDuringTest(tb testing.TB, feature clientfeatures.Feature, featureValue bool) {
	if err := setFeatureDuringTestInternal(tb, feature, featureValue); err != nil {
		tb.Fatal(err)
	}
}

func setFeatureDuringTestInternal(tb testing.TB, feature clientfeatures.Feature, featureValue bool) error {
	// Use the emulation version feature gates for testing so we can set feature gate versions
	// and have them take effect.
	featuresOverridden.Do(func() {
		overrideFeatureGates()
	})

	overriddenFeaturesLock.Lock()
	defer overriddenFeaturesLock.Unlock()

	if runningTest, ok := overriddenFeatures[feature]; ok {
		if !sameTestOrSubtest(tb, runningTest) {
			return fmt.Errorf("feature %q is already overridden by test %q", feature, runningTest)
		}
	} else {
		overriddenFeatures[feature] = tb.Name()
	}

	initialValue := clientfeatures.FeatureGates().Enabled(feature)
	tb.Cleanup(func() {
		overriddenFeaturesLock.Lock()
		defer overriddenFeaturesLock.Unlock()
		delete(overriddenFeatures, feature)

		// restore the feature to its initial value
		if err := clientfeatures.FeatureGates().(featureGatesSetter).Set(feature, initialValue); err != nil {
			tb.Errorf("failed to restore feature %q to %v: %v", feature, initialValue, err)
		}
	})

	return clientfeatures.FeatureGates().(featureGatesSetter).Set(feature, featureValue)
}

func SetEmulatedVersion(tb testing.TB, version *version.Version) {
	featuresOverridden.Do(func() {
		overrideFeatureGates()
	})

	runtime.Must(featureGates.SetEmulationVersion(version))
}

func overrideFeatureGates() {
	featureGates = newTestFeatureGate()
	runtime.Must(clientfeatures.AddVersionedFeaturesToExistingFeatureGates(featureGates))
	clientfeatures.ReplaceFeatureGates(featureGates)
}

// copied from component-base/featuregate/testing
func sameTestOrSubtest(tb testing.TB, testName string) bool {
	return tb.Name() == testName || strings.HasPrefix(tb.Name(), testName+"/")
}

// testFeatureGate implements Gates and VersionedRegistry for testing purposes.
type testFeatureGate struct {
	lock             sync.RWMutex
	known            map[clientfeatures.Feature]clientfeatures.VersionedSpecs
	enabled          map[clientfeatures.Feature]bool
	emulationVersion *version.Version
}

func newTestFeatureGate() *testFeatureGate {
	return &testFeatureGate{
		known:   make(map[clientfeatures.Feature]clientfeatures.VersionedSpecs),
		enabled: make(map[clientfeatures.Feature]bool),
	}
}

func (f *testFeatureGate) AddVersioned(in map[clientfeatures.Feature]clientfeatures.VersionedSpecs) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	for k, v := range in {
		f.known[k] = v
	}
	return nil
}

func (f *testFeatureGate) Add(in map[clientfeatures.Feature]clientfeatures.FeatureSpec) error {
	// Not used by AddVersionedFeaturesToExistingFeatureGates, but required by Registry interface if we implemented it.
	// client-go only calls AddVersioned via AddVersionedFeaturesToExistingFeatureGates.
	return fmt.Errorf("Add not implemented for testFeatureGate")
}

func (f *testFeatureGate) Enabled(key clientfeatures.Feature) bool {
	f.lock.RLock()
	defer f.lock.RUnlock()

	if v, ok := f.enabled[key]; ok {
		return v
	}

	specs, known := f.known[key]
	if !known {
		panic(fmt.Errorf("feature %q is not registered", key))
	}

	// Find the spec for the effective version
	var activeSpec *clientfeatures.FeatureSpec
	if f.emulationVersion == nil {
		// If no emulation version is set, default to the latest version (behavior of envVarFeatureGates)
		if len(specs) > 0 {
			activeSpec = &specs[len(specs)-1]
		}
	} else {
		epoch := f.emulationVersion
		for i := range specs {
			s := &specs[i]
			if s.Version == nil {
				continue
			}
			if s.Version.LessThan(epoch) || s.Version.EqualTo(epoch) {
				if activeSpec == nil || activeSpec.Version.LessThan(s.Version) {
					activeSpec = s
				}
			}
		}
	}

	if activeSpec == nil {
		return false
	}

	return activeSpec.Default
}

func (f *testFeatureGate) Set(key clientfeatures.Feature, value bool) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	if _, ok := f.known[key]; !ok {
		return fmt.Errorf("feature %q is not registered", key)
	}
	f.enabled[key] = value
	return nil
}

func (f *testFeatureGate) SetEmulationVersion(v *version.Version) error {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.emulationVersion = v
	return nil
}

// Ensure interface compliance
var _ clientfeatures.VersionedRegistry = &testFeatureGate{}
var _ clientfeatures.Gates = &testFeatureGate{}
