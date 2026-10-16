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

package options

import (
	"testing"
	"time"

	"github.com/spf13/pflag"
	"k8s.io/component-base/config"
)

func TestBindLeaderElectionFlagsRecoveryDeadline(t *testing.T) {
	t.Run("unset flag leaves RecoveryDeadline nil", func(t *testing.T) {
		var cfg config.LeaderElectionConfiguration
		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		BindLeaderElectionFlags(&cfg, fs)

		if err := fs.Parse([]string{"--leader-elect=true"}); err != nil {
			t.Fatalf("unexpected parse error: %v", err)
		}
		if cfg.RecoveryDeadline != nil {
			t.Fatalf("expected nil RecoveryDeadline when flag is unset, got %v", cfg.RecoveryDeadline)
		}
	})

	t.Run("set flag populates RecoveryDeadline", func(t *testing.T) {
		var cfg config.LeaderElectionConfiguration
		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		BindLeaderElectionFlags(&cfg, fs)

		if err := fs.Parse([]string{"--leader-elect-recovery-deadline=30s"}); err != nil {
			t.Fatalf("unexpected parse error: %v", err)
		}
		if cfg.RecoveryDeadline == nil || cfg.RecoveryDeadline.Duration != 30*time.Second {
			t.Fatalf("expected RecoveryDeadline=30s, got %v", cfg.RecoveryDeadline)
		}
	})
}
