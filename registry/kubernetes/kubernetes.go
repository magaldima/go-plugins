// Package kubernetes provides a kubernetes registry
package kubernetes

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/micro/go-log"
	"github.com/micro/go-plugins/registry/kubernetes/client"

	"github.com/micro/go-micro/cmd"
	"github.com/micro/go-micro/registry"
)

type kregistry struct {
	client  client.Kubernetes
	timeout time.Duration
	options registry.Options
}

const (
	// used on pods as labels & services to select
	// eg: svcSelectorPrefix+"svc.name"
	svcSelectorPrefix = "micro.mu/selector-"
	svcSelectorValue  = "service"

	labelTypeKey          = "micro.mu/type"
	labelTypeValueService = "service"

	// used on k8s services to scope a serialised
	// micro service by pod name
	annotationServiceKeyPrefix = "micro.mu/service-"

	// used on k8s services to scope a serialized
	// micro service
	dataServiceKeyPrefix = "micro.mu/service-"

	// Pod status
	podRunning = "Running"
)

var (
	// label name regex
	labelRe = regexp.MustCompilePOSIX("[-A-Za-z0-9_.]")
)

// secretSelector
var secretSelector = map[string]string{
	labelTypeKey: labelTypeValueService,
}

func init() {
	cmd.DefaultRegistries["kubernetes"] = NewRegistry
}

// serviceName generates a valid service name for k8s labels
func serviceName(name string) string {
	aname := make([]byte, len(name))

	for i, r := range []byte(name) {
		if !labelRe.Match([]byte{r}) {
			aname[i] = '_'
			continue
		}
		aname[i] = r
	}

	return string(aname)
}

// Options returns the registry Options
func (c *kregistry) Options() registry.Options {
	return c.options
}

// Register sets a service selector label and an annotation with a
// serialised version of the service passed in.
// each service has a secret, we need to create one here to register
func (c *kregistry) Register(s *registry.Service, opts ...registry.RegisterOption) error {
	if len(s.Nodes) == 0 {
		return errors.New("you must register at least one node")
	}

	// TODO: grab podname from somewhere better than this.
	podName := os.Getenv("HOSTNAME")
	svcName := serviceName(s.Name)

	// encode micro service
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	svc := string(b)

	secret := &client.Secret{
		Metadata: &client.Meta{
			Name: podName,
			Labels: map[string]*string{
				labelTypeKey:                &labelTypeValueService,
				svcSelectorPrefix + svcName: &svcSelectorValue,
			},
		},
		Data: map[string]string{
			dataServiceKeyPrefix + svcName: svc,
		},
	}

	if _, err := c.client.CreateSecret(podName, secret) != nil {
		return err
	}
	return nil
}

// Deregister nils out any things set in Register
func (c *kregistry) Deregister(s *registry.Service) error {
	if len(s.Nodes) == 0 {
		return errors.New("you must deregister at least one node")
	}

	// TODO: grab podname from somewhere better than this.
	podName := os.Getenv("HOSTNAME")

	if err := c.client.DeleteSecret(podName); err != nil {
		return err
	}
	return nil
}

// GetService will get all the pods with the given service selector,
// and build services from the annotations.
func (c *kregistry) GetService(name string) ([]*registry.Service, error) {
	svcName := serviceName(name)
	secrets, err := c.client.ListSecrets(map[string]string{
		svcSelectorPrefix + svcName: svcSelectorValue,
	})
	if err != nil {
		return nil, err
	}

	if len(secrets.Items) == 0 {
		return nil, registry.ErrNotFound
	}

	svcs := make([]*registry.Service, 0)
	for i, secret := range secrets.Items {
		svcStr, ok := secret.Data[dataServiceKeyPrefix + svcName]
		if !ok {
			continue
		}

		// unmarshal service string
		var svc registry.Service
		err := json.Unmarshal([]byte(*svcStr), &svc)
		if err != nil {
			return nil, fmt.Errorf("could not unmarshal service '%s' from pod annotation", name)
		}
		svcs = append(svcs, &svc)
	}
	return svcs, nil
}

// ListServices will list all the service names
func (c *kregistry) ListServices() ([]*registry.Service, error) {
	secrets, err := c.client.ListSecrets(secretSelector)
	if err != nil {
		return nil, err
	}

	// svcs mapped by name
	svcs := make(map[string]bool)

	for _, secret := range secrets.Items {
		for k, v := range secret.Metadata.Annotations {
			if !strings.HasPrefix(k, dataServiceKeyPrefix) {
				continue
			}

			// we have to unmarshal the annotation itself since the
			// key is encoded to match the regex restriction.
			var svc registry.Service
			if err := json.Unmarshal([]byte(*v), &svc); err != nil {
				continue
			}
			svcs[svc.Name] = true
		}
	}

	var list []*registry.Service
	for val := range svcs {
		list = append(list, &registry.Service{Name: val})
	}
	return list, nil
}

// Watch returns a kubernetes watcher
func (c *kregistry) Watch(opts ...registry.WatchOption) (registry.Watcher, error) {
	return newWatcher(c, opts...)
}

func (c *kregistry) String() string {
	return "kubernetes"
}

// NewRegistry creates a kubernetes registry
func NewRegistry(opts ...registry.Option) registry.Registry {

	var options registry.Options
	for _, o := range opts {
		o(&options)
	}

	// get first host
	var masterURL string
	if len(options.Addrs) > 0 && len(options.Addrs[0]) > 0 {
		masterURL = options.Addrs[0]
	}

	if options.Timeout == 0 {
		options.Timeout = time.Second * 1
	}

	// if no hosts setup, assume InCluster
	/*
	rest, err := client.GetClientConfig(masterURL, "")
	if err != nil {
		log.Fatal(errors.New("unable to get k8s client config"))
	}
	*/
	var c client.Kubernetes
	if len(host) == 0 {
		c = client.NewClientInCluster()
	} else {
		c = client.NewClientByHost(host)
	}

	return &kregistry{
		client:  c,
		options: options,
		timeout: options.Timeout,
	}
}
