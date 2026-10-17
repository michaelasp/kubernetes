/*
Copyright 2023 The Kubernetes Authors.

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

package app

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/sets"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	cloudprovider "k8s.io/cloud-provider"
	cloudcontrollerconfig "k8s.io/cloud-provider/app/config"
	"k8s.io/cloud-provider/names"
	"k8s.io/controller-manager/pkg/clientbuilder"
)

func TestCloudControllerNamesConsistency(t *testing.T) {
	controllerNameRegexp := regexp.MustCompile("^[a-z]([-a-z]*[a-z])?$")

	for name := range DefaultInitFuncConstructors {
		if !controllerNameRegexp.MatchString(name) {
			t.Errorf("name consistency check failed: controller %q must consist of lower case alphabetic characters or '-', and must start and end with an alphabetic character", name)
		}
		if !strings.HasSuffix(name, "-controller") {
			t.Errorf("name consistency check failed: controller %q must have \"-controller\" suffix", name)
		}
	}
}

func TestCloudControllerNamesDeclaration(t *testing.T) {
	declaredControllers := sets.New(
		names.CloudNodeController,
		names.ServiceLBController,
		names.NodeRouteController,
		names.CloudNodeLifecycleController,
	)

	for name := range DefaultInitFuncConstructors {
		if !declaredControllers.Has(name) {
			t.Errorf("name declaration check failed: controller name %q should be declared in  \"controller_names.go\" and added to this test", name)
		}
	}
}

// bareClientBuilder implements only the cloud provider's builder interface,
// standing in for an out-of-tree builder without discovery clients.
type bareClientBuilder struct {
	config *restclient.Config
}

func (b bareClientBuilder) Config(name string) (*restclient.Config, error) {
	cfg := *b.config
	return restclient.AddUserAgent(&cfg, name), nil
}
func (b bareClientBuilder) ConfigOrDie(name string) *restclient.Config {
	cfg, _ := b.Config(name)
	return cfg
}
func (b bareClientBuilder) Client(name string) (clientset.Interface, error) {
	return clientset.NewForConfig(b.ConfigOrDie(name))
}
func (b bareClientBuilder) ClientOrDie(name string) clientset.Interface {
	return clientset.NewForConfigOrDie(b.ConfigOrDie(name))
}

func TestGatedClientBuilders(t *testing.T) {
	// Every request gets a service account back, which is all the dynamic
	// client builder needs to hand out per-controller clients.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"kind":"ServiceAccount","apiVersion":"v1","metadata":{"name":"x","namespace":"kube-system"}}`))
	}))
	defer server.Close()

	for _, useServiceAccounts := range []bool{false, true} {
		t.Run(fmt.Sprintf("serviceAccountCredentials=%v", useServiceAccounts), func(t *testing.T) {
			kubeconfig := &restclient.Config{Host: server.URL}
			c := &cloudcontrollerconfig.Config{Kubeconfig: kubeconfig, Client: clientset.NewForConfigOrDie(kubeconfig)}
			c.ComponentConfig.KubeCloudShared.UseServiceAccountCredentials = useServiceAccounts

			wraps := 0
			gate := func(rt http.RoundTripper) http.RoundTripper {
				wraps++
				return rt
			}

			rootClientBuilder, controllerClientBuilder := gatedClientBuilders(c.Complete(), gate)

			// Building a client applies the gate to its transport.
			rootClientBuilder.ClientOrDie("root")
			controllerClientBuilder.ClientOrDie("controller")
			if wraps != 2 {
				t.Errorf("gate applied %d times building a root and a controller client, want 2", wraps)
			}
			if kubeconfig.WrapTransport != nil {
				t.Errorf("the controller manager's kubeconfig must not be modified")
			}

			if useServiceAccounts {
				if _, ok := controllerClientBuilder.(*clientbuilder.DynamicControllerClientBuilder); !ok {
					t.Errorf("controller builder is %T, want the service account builder", controllerClientBuilder)
				}
			} else if got, ok := controllerClientBuilder.(clientbuilder.SimpleControllerClientBuilder); !ok || got != rootClientBuilder {
				t.Errorf("controller builder is %T, want the root builder", controllerClientBuilder)
			}
		})
	}
}

func TestControllerManagerClientBuilder(t *testing.T) {
	cfg := &restclient.Config{Host: "https://localhost:6443"}

	simple := clientbuilder.SimpleControllerClientBuilder{ClientConfig: cfg}
	if got := controllerManagerClientBuilder(simple); got != cloudprovider.ControllerClientBuilder(simple) {
		t.Errorf("a controller manager builder must be returned as is, got %#v", got)
	}

	adapted := controllerManagerClientBuilder(bareClientBuilder{config: cfg})
	if _, err := adapted.DiscoveryClient("controller"); err != nil {
		t.Errorf("adapted builder cannot build a discovery client: %v", err)
	}
	if adapted.DiscoveryClientOrDie("controller") == nil {
		t.Errorf("adapted builder returned a nil discovery client")
	}
}
