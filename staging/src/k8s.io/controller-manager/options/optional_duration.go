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
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// optionalDurationValue is a pflag.Value for a *metav1.Duration that stays nil
// unless the flag is set, so that "unset" and "zero" are distinguishable.
type optionalDurationValue struct {
	target **metav1.Duration
}

func (d *optionalDurationValue) String() string {
	if d.target == nil || *d.target == nil {
		return ""
	}
	return (*d.target).Duration.String()
}

func (d *optionalDurationValue) Set(s string) error {
	v, err := time.ParseDuration(s)
	if err != nil {
		return err
	}
	*d.target = &metav1.Duration{Duration: v}
	return nil
}

func (d *optionalDurationValue) Type() string {
	return "duration"
}
