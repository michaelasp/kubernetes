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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestOptionalDurationValue(t *testing.T) {
	t.Run("unset flag leaves the target nil", func(t *testing.T) {
		var target *metav1.Duration
		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		fs.Var(&optionalDurationValue{target: &target}, "deadline", "")
		if err := fs.Parse(nil); err != nil {
			t.Fatalf("unexpected parse error: %v", err)
		}
		if target != nil {
			t.Fatalf("expected nil target when flag is unset, got %v", target)
		}
	})

	t.Run("set flag populates the target", func(t *testing.T) {
		var target *metav1.Duration
		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		fs.Var(&optionalDurationValue{target: &target}, "deadline", "")
		if err := fs.Parse([]string{"--deadline=30s"}); err != nil {
			t.Fatalf("unexpected parse error: %v", err)
		}
		if target == nil || target.Duration != 30*time.Second {
			t.Fatalf("expected 30s, got %v", target)
		}
	})

	t.Run("invalid value is rejected", func(t *testing.T) {
		var target *metav1.Duration
		fs := pflag.NewFlagSet("test", pflag.ContinueOnError)
		fs.Var(&optionalDurationValue{target: &target}, "deadline", "")
		if err := fs.Parse([]string{"--deadline=soon"}); err == nil {
			t.Fatal("expected a parse error for an invalid duration")
		}
	})
}
