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

package controllermanager

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/test/integration/framework"
)

// TestMain runs the goroutine leak check for this package, like every other
// integration package that embeds a kube-apiserver. The pre-existing test in
// this package was never covered by it.
func TestMain(m *testing.M) {
	framework.EtcdMain(m.Run)
}

// TestLeaderElectionRecoveryWriteGate verifies that a gated client's write is
// rejected with ErrWriteGateClosed while the elector is inactive, and succeeds
// after the gate reopens.
func TestLeaderElectionRecoveryWriteGate(t *testing.T) {
	apiServer := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	t.Cleanup(apiServer.TearDownFn)

	var failLeaseRequests atomic.Bool
	lockConfig := restclient.CopyConfig(apiServer.ClientConfig)
	lockConfig.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if failLeaseRequests.Load() && strings.Contains(req.URL.Path, "/leases") {
				return &http.Response{
					StatusCode: http.StatusServiceUnavailable,
					Header:     make(http.Header),
					Body:       http.NoBody,
					Request:    req,
				}, nil
			}
			return rt.RoundTrip(req)
		})
	})

	rl, err := resourcelock.NewFromKubeconfig(
		resourcelock.LeasesResourceLock,
		metav1.NamespaceSystem,
		"recovery-writegate-test",
		resourcelock.ResourceLockConfig{
			Identity: "holder-1",
		},
		lockConfig,
		500*time.Millisecond,
	)
	if err != nil {
		t.Fatalf("failed to create resource lock: %v", err)
	}

	startedCh := make(chan struct{})
	stoppedCh := make(chan struct{}, 4)

	le, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: 2 * time.Second,
		RenewDeadline: 600 * time.Millisecond,
		RetryPeriod:   100 * time.Millisecond,
		Name:          "recovery-writegate-test",
		Recovery:      &leaderelection.RecoveryConfig{},
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				close(startedCh)
			},
			OnStoppedLeading: func() {
				select {
				case stoppedCh <- struct{}{}:
				default:
				}
			},
		},
	})
	if err != nil {
		t.Fatalf("failed to create LeaderElector: %v", err)
	}

	gatedConfig := restclient.CopyConfig(apiServer.ClientConfig)
	gatedConfig.Wrap(le.WriteGate())

	gatedClient, err := kubernetes.NewForConfig(gatedConfig)
	if err != nil {
		t.Fatalf("failed to create gated client: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Before starting election, the write gate is closed: reads pass, writes fail with ErrWriteGateClosed.
	if _, err := gatedClient.CoreV1().Namespaces().Get(ctx, metav1.NamespaceSystem, metav1.GetOptions{}); err != nil {
		t.Fatalf("expected GET through closed write gate to succeed, got: %v", err)
	}
	_, err = gatedClient.CoreV1().ConfigMaps(metav1.NamespaceDefault).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "rejected-before-leading"},
	}, metav1.CreateOptions{})
	if !errors.Is(err, leaderelection.ErrWriteGateClosed) {
		t.Fatalf("expected ErrWriteGateClosed before leading, got: %v", err)
	}

	go le.Run(ctx)

	select {
	case <-startedCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for OnStartedLeading")
	}

	// Simulate lease renewal outage so the renew deadline expires and the write gate closes.
	failLeaseRequests.Store(true)
	select {
	case <-stoppedCh:
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for OnStoppedLeading after lease renewal failure")
	}

	// Direct write on the gated client must be rejected with ErrWriteGateClosed while inactive.
	_, err = gatedClient.CoreV1().ConfigMaps(metav1.NamespaceDefault).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "rejected-while-inactive"},
	}, metav1.CreateOptions{})
	if !errors.Is(err, leaderelection.ErrWriteGateClosed) {
		t.Fatalf("expected ErrWriteGateClosed while inactive, got: %v", err)
	}

	// Restore lease renewals so the elector recovers the lease and reopens the write gate.
	failLeaseRequests.Store(false)

	// Gated client write succeeds once the gate reopens.
	err = wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		_, createErr := gatedClient.CoreV1().ConfigMaps(metav1.NamespaceDefault).Create(ctx, &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Name: "succeeded-after-recovery"},
		}, metav1.CreateOptions{})
		if errors.Is(createErr, leaderelection.ErrWriteGateClosed) {
			return false, nil
		}
		return createErr == nil || apierrors.IsAlreadyExists(createErr), createErr
	})
	if err != nil {
		t.Fatalf("expected gated client write to succeed after gate reopened: %v", err)
	}
}

func waitForDefaultServiceAccount(ctx context.Context, client kubernetes.Interface, namespace string, timeout time.Duration) error {
	return wait.PollUntilContextTimeout(ctx, 50*time.Millisecond, timeout, true, func(ctx context.Context) (bool, error) {
		_, err := client.CoreV1().ServiceAccounts(namespace).Get(ctx, "default", metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return err == nil, err
	})
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
