package kubernetes

import (
	"encoding/json"
	"errors"
	"strings"

	"github.com/micro/go-log"
	"github.com/micro/go-micro/registry"
	"github.com/micro/go-plugins/registry/kubernetes/client"
	"github.com/micro/go-plugins/registry/kubernetes/client/watch"
)

type k8sWatcher struct {
	registry *kregistry
	watcher  watch.Watch
	next     chan *registry.Result
}

func (k *k8sWatcher) buildSecretResults(secret *client.Secret, action string) []*registry.Result {
	var results []*registry.Result

	if secret.Metadata != nil {
		for ak, av := range secret.Metadata.Annotations {
			// check this annotation kv is a service notation
			if !strings.HasPrefix(ak, dataServiceKeyPrefix) {
				continue
			}
			if av == nil {
				continue
			}
			rslt := &registry.Result{Action: action}
			// unmarshal service notation from annotation value
			err := json.Unmarshal([]byte(*av), &rslt.Service)
			if err != nil {
				continue
			}
			results = append(results, rslt)
		}
	}
	return results
}

// handleEvent will taken an event from the k8s secret API and do the correct
// things with the result, based on the local cache.
func (k *k8sWatcher) handleEvent(event watch.Event) {
	var secret client.Secret
	if err := json.Unmarshal([]byte(event.Object), &secret); err != nil {
		log.Log("K8s Watcher: Couldnt unmarshal event object from secret")
		return
	}

	var results []*registry.Result
	switch event.Type {
	case watch.Added:
		// Secret was added
		results = k.buildSecretResults(&secret, "create")
	case watch.Deleted:
		// Secret was deleted
		// passing in cache might not return all results
		results = k.buildSecretResults(&secret, "delete")
	}
	for _, result := range results {
		k.next <- result
	}
	return

}

// Next will block until a new result comes in
func (k *k8sWatcher) Next() (*registry.Result, error) {
	r, ok := <-k.next
	if !ok {
		return nil, errors.New("result chan closed")
	}
	return r, nil
}

// Stop will cancel any requests, and close channels
func (k *k8sWatcher) Stop() {
	k.watcher.Stop()

	select {
	case <-k.next:
		return
	default:
		close(k.next)
	}
}

func newWatcher(kr *kregistry, opts ...registry.WatchOption) (registry.Watcher, error) {
	var wo registry.WatchOptions
	for _, o := range opts {
		o(&wo)
	}

	selector := secretSelector
	if len(wo.Service) > 0 {
		selector = map[string]string{
			svcSelectorPrefix + serviceName(wo.Service): svcSelectorValue,
		}
	}

	// Create watch request
	watcher, err := kr.client.WatchSecrets(selector)
	if err != nil {
		return nil, err
	}

	k := &k8sWatcher{
		registry: kr,
		watcher:  watcher,
		next:     make(chan *registry.Result),
		secrets:  make(map[string]*client.Secret),
	}

	// range over watch request changes, and invoke
	// the update event
	go func() {
		for event := range watcher.ResultChan() {
			k.handleEvent(event)
		}
		k.Stop()
	}()

	return k, nil
}
