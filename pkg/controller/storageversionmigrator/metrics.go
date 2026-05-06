/*
Copyright 2026 The Kubernetes Authors.

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

package storageversionmigrator

import (
	"sync"

	"k8s.io/component-base/metrics"
	"k8s.io/component-base/metrics/legacyregistry"
)

const Subsystem = "storage_migrator_core_migrator"

var (
	MigratedObjects = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      Subsystem,
			Name:           "migrated_objects",
			Help:           "Total number of objects migrated in the current migration.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"group", "resource"},
	)
	RemainingObjects = metrics.NewGaugeVec(
		&metrics.GaugeOpts{
			Subsystem:      Subsystem,
			Name:           "remaining_objects",
			Help:           "Total number of objects remaining to be migrated in the current migration.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"group", "resource"},
	)
	MigrationsTotal = metrics.NewCounterVec(
		&metrics.CounterOpts{
			Subsystem:      Subsystem,
			Name:           "migrations_total",
			Help:           "Total number of migrations attempted.",
			StabilityLevel: metrics.ALPHA,
		},
		[]string{"group", "resource"},
	)
)

var registerMetrics sync.Once

func Register() {
	registerMetrics.Do(func() {
		legacyregistry.MustRegister(MigratedObjects)
		legacyregistry.MustRegister(RemainingObjects)
		legacyregistry.MustRegister(MigrationsTotal)
	})
}
