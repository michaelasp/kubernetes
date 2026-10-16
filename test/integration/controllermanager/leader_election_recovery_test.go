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
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	kubecontrollermanagertesting "k8s.io/kubernetes/cmd/kube-controller-manager/app/testing"
	serviceaccountcontroller "k8s.io/kubernetes/pkg/controller/serviceaccount"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/kubeconfig"
)

func TestMain(m *testing.M) {
	framework.EtcdMain(m.Run)
}

// TestLeaderElectionRecoveryWriteGateAndControllerPause verifies:
//  1. A gated client's write is rejected with ErrWriteGateClosed while the elector
//     is inactive, and succeeds after the gate reopens.
//  2. A controller using workqueue pause stops dequeuing while the gate is closed
//     and reconciles the backlog after the gate reopens.
func TestLeaderElectionRecoveryWriteGateAndControllerPause(t *testing.T) {
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.LeaderElectionRecovery, true)

	apiServer := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	t.Cleanup(apiServer.TearDownFn)

	adminClient, err := kubernetes.NewForConfig(apiServer.ClientConfig)
	if err != nil {
		t.Fatalf("failed to create admin client: %v", err)
	}

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

	var le *leaderelection.LeaderElector
	le, err = leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          rl,
		LeaseDuration: 2 * time.Second,
		RenewDeadline: 600 * time.Millisecond,
		RetryPeriod:   100 * time.Millisecond,
		Name:          "recovery-writegate-test",
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

	var controllerWriteAttemptsWhileClosed atomic.Int32
	var gateClosed atomic.Bool
	gateClosed.Store(true)

	gatedConfig := restclient.CopyConfig(apiServer.ClientConfig)
	gatedConfig.Wrap(func(rt http.RoundTripper) http.RoundTripper {
		return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			if gateClosed.Load() && req.Method != http.MethodGet && req.Method != http.MethodHead {
				controllerWriteAttemptsWhileClosed.Add(1)
			}
			return rt.RoundTrip(req)
		})
	})
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

	informerFactory := informers.NewSharedInformerFactory(gatedClient, 0)
	sac, err := serviceaccountcontroller.NewServiceAccountsController(
		ctx,
		informerFactory.Core().V1().ServiceAccounts(),
		informerFactory.Core().V1().Namespaces(),
		gatedClient,
		serviceaccountcontroller.DefaultServiceAccountsControllerOptions(),
	)
	if err != nil {
		t.Fatalf("failed to create ServiceAccountsController: %v", err)
	}
	le.RegisterPausable(sac)

	go le.Run(ctx)

	select {
	case <-startedCh:
		gateClosed.Store(false)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for OnStartedLeading")
	}

	informerFactory.Start(ctx.Done())
	go sac.Run(ctx, 1)

	// Verify initial reconciliation while gate is open.
	if _, err := adminClient.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "ns-before-outage"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create ns-before-outage: %v", err)
	}
	if err := waitForDefaultServiceAccount(ctx, adminClient, "ns-before-outage", 10*time.Second); err != nil {
		t.Fatalf("expected default ServiceAccount in ns-before-outage: %v", err)
	}

	// Simulate lease renewal outage so the renew deadline expires and the write gate closes.
	failLeaseRequests.Store(true)
	select {
	case <-stoppedCh:
		gateClosed.Store(true)
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for OnStoppedLeading after lease renewal failure")
	}
	controllerWriteAttemptsWhileClosed.Store(0)

	// Direct write on the gated client must be rejected with ErrWriteGateClosed while inactive.
	_, err = gatedClient.CoreV1().ConfigMaps(metav1.NamespaceDefault).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "rejected-while-inactive"},
	}, metav1.CreateOptions{})
	if !errors.Is(err, leaderelection.ErrWriteGateClosed) {
		t.Fatalf("expected ErrWriteGateClosed while inactive, got: %v", err)
	}

	// Create a namespace while the gate is closed; the paused controller must not dequeue/write yet.
	if _, err := adminClient.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "ns-during-outage"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create ns-during-outage: %v", err)
	}
	time.Sleep(300 * time.Millisecond)
	if _, err := adminClient.CoreV1().ServiceAccounts("ns-during-outage").Get(ctx, "default", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected default ServiceAccount not to be created while paused, got err=%v", err)
	}
	if attempts := controllerWriteAttemptsWhileClosed.Load(); attempts != 0 {
		t.Fatalf("expected 0 write attempts bypassing pause while gate was closed, got %d", attempts)
	}

	// Restore lease renewals so the elector recovers the lease and reopens the write gate.
	gateClosed.Store(false)
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

	// Paused controller resumes and reconciles the backlog item (ns-during-outage).
	if err := waitForDefaultServiceAccount(ctx, adminClient, "ns-during-outage", 10*time.Second); err != nil {
		t.Fatalf("expected default ServiceAccount in ns-during-outage after recovery: %v", err)
	}
}

// TestKubeControllerManagerLeaderElectionRecovery verifies that kube-controller-manager
// continues to run across a lease renewal interruption, pauses reconciliation while
// its write gate is closed, and recovers leadership without restarting once the
// interruption ends.
func TestKubeControllerManagerLeaderElectionRecovery(t *testing.T) {
	apiServer := kubeapiservertesting.StartTestServerOrDie(t, nil, framework.DefaultTestServerFlags(), framework.SharedEtcd())
	t.Cleanup(apiServer.TearDownFn)

	client, err := kubernetes.NewForConfig(apiServer.ClientConfig)
	if err != nil {
		t.Fatalf("failed to create client: %v", err)
	}

	targetURL, err := url.Parse(apiServer.ClientConfig.Host)
	if err != nil {
		t.Fatalf("failed to parse apiserver URL: %v", err)
	}
	rt, err := restclient.TransportFor(apiServer.ClientConfig)
	if err != nil {
		t.Fatalf("failed to create apiserver transport: %v", err)
	}

	var failLeaseRenewals atomic.Bool
	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	proxy.Transport = rt
	proxy.FlushInterval = -1

	proxyServer := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if failLeaseRenewals.Load() && strings.Contains(r.URL.Path, "/apis/coordination.k8s.io/v1/namespaces/kube-system/leases/kube-controller-manager") {
			http.Error(w, "simulated apiserver lease unavailability", http.StatusServiceUnavailable)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(proxyServer.Close)

	kcmClientConfig := restclient.CopyConfig(apiServer.ClientConfig)
	kcmClientConfig.Host = proxyServer.URL
	kcmClientConfig.TLSClientConfig.Insecure = true
	kcmClientConfig.TLSClientConfig.CAFile = ""
	kcmClientConfig.TLSClientConfig.CAData = nil

	kubeConfig := kubeconfig.CreateKubeConfig(kcmClientConfig)
	kubeConfigFile := filepath.Join(t.TempDir(), "kubeconfig.yaml")
	if err := clientcmd.WriteToFile(*kubeConfig, kubeConfigFile); err != nil {
		t.Fatalf("failed to write kubeconfig: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	kcm, err := kubecontrollermanagertesting.StartTestServer(t, ctx, []string{
		"--kubeconfig=" + kubeConfigFile,
		"--controllers=serviceaccount",
		"--leader-elect=true",
		"--leader-elect-lease-duration=4s",
		"--leader-elect-renew-deadline=1500ms",
		"--leader-elect-retry-period=200ms",
		"--leader-elect-recovery-deadline=30s",
		"--feature-gates=LeaderElectionRecovery=true,ControllerManagerReleaseLeaderElectionLockOnExit=true",
	})
	if err != nil {
		t.Fatalf("failed to start KCM: %v", err)
	}
	t.Cleanup(kcm.TearDownFn)

	// Wait for KCM to acquire the lease and reconcile an initial namespace.
	var initialLease *coordinationv1.Lease
	err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		lease, getErr := client.CoordinationV1().Leases(metav1.NamespaceSystem).Get(ctx, "kube-controller-manager", metav1.GetOptions{})
		if getErr != nil {
			return false, nil
		}
		if lease.Spec.HolderIdentity != nil && *lease.Spec.HolderIdentity != "" {
			initialLease = lease
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("timed out waiting for KCM lease acquisition: %v", err)
	}

	if _, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "kcm-recovery-ns-1"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create kcm-recovery-ns-1: %v", err)
	}
	if err := waitForDefaultServiceAccount(ctx, client, "kcm-recovery-ns-1", 15*time.Second); err != nil {
		t.Fatalf("timed out waiting for default ServiceAccount in kcm-recovery-ns-1: %v", err)
	}

	// Block lease renewals for longer than RenewDeadline (1.5s) so KCM enters recovery mode.
	failLeaseRenewals.Store(true)
	time.Sleep(2 * time.Second)

	// Create a second namespace while KCM's write gate is closed.
	if _, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: "kcm-recovery-ns-2"},
	}, metav1.CreateOptions{}); err != nil {
		t.Fatalf("failed to create kcm-recovery-ns-2: %v", err)
	}
	time.Sleep(400 * time.Millisecond)
	if _, err := client.CoreV1().ServiceAccounts("kcm-recovery-ns-2").Get(ctx, "default", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("expected default ServiceAccount in kcm-recovery-ns-2 not to exist while KCM write gate is closed, got err=%v", err)
	}

	// Restore lease renewals and verify KCM recovers the same lease and reconciles kcm-recovery-ns-2 without restarting.
	beforeRecoveryRenewTime := initialLease.Spec.RenewTime.DeepCopy()
	failLeaseRenewals.Store(false)

	err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 15*time.Second, true, func(ctx context.Context) (bool, error) {
		lease, getErr := client.CoordinationV1().Leases(metav1.NamespaceSystem).Get(ctx, "kube-controller-manager", metav1.GetOptions{})
		if getErr != nil {
			return false, nil
		}
		if lease.Spec.HolderIdentity == nil || *lease.Spec.HolderIdentity != *initialLease.Spec.HolderIdentity {
			t.Fatalf("expected lease holder identity %q to be preserved across recovery, got %v", *initialLease.Spec.HolderIdentity, lease.Spec.HolderIdentity)
		}
		return lease.Spec.RenewTime != nil && lease.Spec.RenewTime.After(beforeRecoveryRenewTime.Time), nil
	})
	if err != nil {
		t.Fatalf("timed out waiting for KCM to renew lease after recovery: %v", err)
	}

	if err := waitForDefaultServiceAccount(ctx, client, "kcm-recovery-ns-2", 15*time.Second); err != nil {
		t.Fatalf("timed out waiting for KCM to reconcile kcm-recovery-ns-2 after recovery: %v", err)
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
