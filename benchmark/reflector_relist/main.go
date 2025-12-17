package main

import (
	"context"
	"fmt"
	"net/http"
	"path/filepath"
	"time"

	flag "github.com/spf13/pflag"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

// MeasurableStore wraps a cache.Store and records Replace latency
type MeasurableStore struct {
	cache.Store
	replaceLatencies []time.Duration
	latencyInjection time.Duration
}

func (s *MeasurableStore) Replace(items []interface{}, resourceVersion string) error {
	fmt.Printf("MeasurableStore: Starting Replace (holding lock for %v)...\n", s.latencyInjection)
	start := time.Now()
	// Simulate the "internal FIFO store" work that holds the lock
	if s.latencyInjection > 0 {
		time.Sleep(s.latencyInjection)
	}
	err := s.Store.Replace(items, resourceVersion)
	duration := time.Since(start)
	s.replaceLatencies = append(s.replaceLatencies, duration)
	fmt.Printf("MeasurableStore: Replace took: %v (Items: %d)\n", duration, len(items))
	return err
}

func main() {
	kubeconfig := flag.String("kubeconfig", filepath.Join(homedir.HomeDir(), ".kube", "config"), "absolute path to the kubeconfig file")
	namespace := flag.String("namespace", "default", "namespace to watch")
	latency := flag.Duration("latency", 0, "latency to inject into Replace (simulating blocking)")
	churn := flag.Bool("churn", false, "enable background churn (updates) to overflow watch buffer")
	flag.Parse()

	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err)
	}
	// Disable client-side throttling to allow massive churn
	config.QPS = -1
	config.Burst = -1

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err)
	}

	fmt.Println("Warning: This benchmark assumes resources are already present in the cluster.")
	fmt.Println("Use benchmark/list/main.go --create ... to populate things first.")

	if *churn {
		fmt.Println("Starting background churn (updating existing pods)...")
		go startChurn(clientset, *namespace)
	}

	baseStore := cache.NewStore(cache.MetaNamespaceKeyFunc)
	measurableStore := &MeasurableStore{
		Store:            baseStore,
		latencyInjection: *latency,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lw := &cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			fmt.Println("List called...")
			return clientset.CoreV1().Pods(*namespace).List(ctx, options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			fmt.Printf("Watch called (RV=%s)...\n", options.ResourceVersion)
			// Simulate "Zero History" - if we try to resume (RV set), we fail with 410 Gone.
			// This guarantees that if the *initial* watch dies (due to Slow Replace), we loop.
			if options.ResourceVersion != "" && options.ResourceVersion != "0" {
				fmt.Println("Simulating 410 Gone (Zero History)...")
				return nil, &apierrors.StatusError{ErrStatus: metav1.Status{
					Status:  metav1.StatusFailure,
					Code:    http.StatusGone,
					Reason:  metav1.StatusReasonExpired,
					Message: "Resource version too old (simulated)",
				}}
			}
			return clientset.CoreV1().Pods(*namespace).Watch(ctx, options)
		},
	}

	reflector := cache.NewReflector(lw, &v1.Pod{}, measurableStore, 0)

	// Run continuously to see if it loops
	fmt.Println("Starting Reflector. Press Ctrl+C to stop.")
	go reflector.Run(ctx.Done())

	// Block forever (or until user kills it)
	select {}
}

func startChurn(clientset *kubernetes.Clientset, namespace string) {
	// List pods once to get names
	pods, err := clientset.CoreV1().Pods(namespace).List(context.Background(), metav1.ListOptions{})
	if err != nil {
		fmt.Printf("Churn failed to list pods: %v\n", err)
		return
	}
	if len(pods.Items) == 0 {
		fmt.Println("Churn: No pods found to update.")
		return
	}

	// Update loop - parallelize to ensure we fill the watch cache
	concurrency := 20
	for i := 0; i < concurrency; i++ {
		go func(id int) {
			for {
				// Pick a random pod to update to avoid contention on the same object if possible,
				// or just iterate strided.
				for j := id; j < len(pods.Items); j += concurrency {
					p := pods.Items[j]
					// Update label
					if p.Labels == nil {
						p.Labels = make(map[string]string)
					}
					p.Labels["churn"] = fmt.Sprintf("%d-%d", time.Now().UnixNano(), id)
					_, err := clientset.CoreV1().Pods(namespace).Update(context.Background(), &p, metav1.UpdateOptions{})
					if err != nil {
						// Ignore errors (might be conflicts, etc)
					}
				}
				// No sleep, full throttle
			}
		}(i)
	}
	// Blocks forever
	select {}
}
