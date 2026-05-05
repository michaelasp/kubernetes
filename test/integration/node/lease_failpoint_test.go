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

package node

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	coordv1 "k8s.io/api/coordination/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/failpoint"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/kubernetes/pkg/controller/nodelifecycle"
	testutils "k8s.io/kubernetes/test/integration/util"
)

// TestLeaseCircuitBreakerWithFailpoints verifies that when Lease Watch stream is halted,
// the NodeLifecycleController executes the direct-to-api GET fallback logic automatically.
func TestLeaseCircuitBreakerWithFailpoints(t *testing.T) {
	if !failpoint.Enabled {
		t.Skip("Skipping because failpoints are not enabled; run with -tags=failpoints")
	}
	var directGetHits int32
	blockLeaseWatch := make(chan struct{})
	defer close(blockLeaseWatch)

	nodeName := "failpoint-target-node"

	// 1. Failpoint: Pause watch consumption ONLY if it contains the Lease object FOR OUR SPECIFIC TARGET NODE
	// This allows system internal leases to continue working normally, preventing bootstrapping deadlock.
	failpoint.Register("BeforeProcessWatchEvent", func(arg interface{}) {
		if l, ok := arg.(*coordv1.Lease); ok && l.Name == nodeName {
			// Suspend the specific reflector goroutine indefinitely until test finishes
			<-blockLeaseWatch
		}
	})
	defer failpoint.Unregister("BeforeProcessWatchEvent")

	// 3. Failpoint: Track successful execution of our minimal direct GET code block
	failpoint.Register("NodeLifecycleDirectGetFallbackHit", func(arg interface{}) {
		atomic.AddInt32(&directGetHits, 1)
	})
	defer failpoint.Unregister("NodeLifecycleDirectGetFallbackHit")

	// Standard Integration API initialization
	testCtx := testutils.InitTestAPIServer(t, "lease-failpoint-test", nil)
	cs := testCtx.ClientSet

	// Setup clientsets & informers
	externalClientConfig := restclient.CopyConfig(testCtx.KubeConfig)
	externalClientConfig.QPS = -1
	externalClientset := clientset.NewForConfigOrDie(externalClientConfig)
	externalInformers := informers.NewSharedInformerFactory(externalClientset, time.Second)

	// Use strict timing: 2s stale duration with rapid checks
	nodeGrace := 2 * time.Second
	monitorPeriod := 100 * time.Millisecond

	nc, err := nodelifecycle.NewNodeLifecycleController(
		testCtx.Ctx,
		externalInformers.Coordination().V1().Leases(),
		externalInformers.Core().V1().Pods(),
		externalInformers.Core().V1().Nodes(),
		externalInformers.Apps().V1().DaemonSets(),
		cs,
		nodeGrace,
		nodeGrace,   // same startup grace period
		monitorPeriod,
		100, 100, 50, 0.55,
	)
	if err != nil {
		t.Fatalf("Failed to create node controller: %v", err)
	}

	// Start informers and sync caches normally (Initial Lists proceed successfully)
	externalInformers.Start(testCtx.Ctx.Done())
	externalInformers.WaitForCacheSync(testCtx.Ctx.Done())

	// Run controller loop asynchronously
	go nc.Run(testCtx.Ctx)

	// Create the targeted Node object
	node := &v1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name:   nodeName,
			Labels: map[string]string{"node.kubernetes.io/exclude-disruption": "true"},
		},
		Spec: v1.NodeSpec{},
		Status: v1.NodeStatus{
			Conditions: []v1.NodeCondition{
				{Type: v1.NodeReady, Status: v1.ConditionTrue},
			},
		},
	}
	_, err = cs.CoreV1().Nodes().Create(testCtx.Ctx, node, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to initialize node: %v", err)
	}

	// Ensure lease namespace exists (integration API env is minimal)
	nsObj := &v1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: v1.NamespaceNodeLease}}
	_, err = cs.CoreV1().Namespaces().Create(testCtx.Ctx, nsObj, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		t.Fatalf("Failed to ensure namespace %q: %v", v1.NamespaceNodeLease, err)
	}

	// Pre-create the initial Lease object via Direct API Write!
	// This will trigger a WATCH event that gets BLOCKED by our failpoint,
	// so it stays in the API server but NEVER reaches the Informer cache!
	duration := int32(40)
	testLease := &coordv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      nodeName,
			Namespace: v1.NamespaceNodeLease,
		},
		Spec: coordv1.LeaseSpec{
			HolderIdentity:       &nodeName,
			LeaseDurationSeconds: &duration,
			RenewTime:            &metav1.MicroTime{Time: time.Now()},
		},
	}
	_, err = cs.CoordinationV1().Leases(v1.NamespaceNodeLease).Create(testCtx.Ctx, testLease, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to initialize lease object directly: %v", err)
	}

	// The controller initialized at creation T=0. Wait for 2s grace window to elapse.
	// Once expired, and with our failpoint blocking WATCH updates, the node controller
	// SHOULD execute the direct GET fallback block.
	t.Log("Waiting for NodeGrace period to elapse and trigger the Direct GET logic via failpoint.")

	err = wait.PollUntilContextTimeout(testCtx.Ctx, 500*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		count := atomic.LoadInt32(&directGetHits)
		if count > 0 {
			return true, nil
		}
		return false, nil
	})

	if err != nil {
		t.Errorf("FAIL: Node lifecycle controller did not trigger direct GET fallback code path within timeout.")
	} else {
		t.Logf("SUCCESS: Failpoint captured %d hits to direct GET fallback code path!", atomic.LoadInt32(&directGetHits))
	}
}
