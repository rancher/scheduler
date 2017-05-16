package resourcewatchers

import (
	"fmt"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/go-rancher-metadata/metadata"
	"github.com/rancher/go-rancher/v2"
	"github.com/rancher/scheduler/scheduler"
)

func WatchMetadata(client metadata.Client, updater scheduler.ResourceUpdater, rclient *client.RancherClient) error {
	logrus.Infof("Subscribing to metadata changes.")

	watcher := &metadataWatcher{
		resourceUpdater: updater,
		client:          client,
		knownHosts:      map[string]bool{},
		previousIPs:     map[string]string{},
		rclient:         rclient,
	}
	return client.OnChangeWithError(5, watcher.updateFromMetadata)
}

type metadataWatcher struct {
	resourceUpdater       scheduler.ResourceUpdater
	client                metadata.Client
	consecutiveErrorCount int
	initialized           bool
	knownHosts            map[string]bool
	mu                    sync.Mutex
	previousIPs           map[string]string
	rclient               *client.RancherClient
}

const (
	instancePool            string = "instanceReservation"
	memoryPool              string = "memoryReservation"
	cpuPool                 string = "cpuReservation"
	storageSize             string = "storageSize"
	totalAvailableInstances int64  = 1000000
	hostLabels              string = "hostLabels"
	ipLabel                 string = "io.rancher.scheduler.ips"
	schedulerUpdate         string = "scheduler.update"
)

func (w *metadataWatcher) updateFromMetadata(mdVersion string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	hosts, err := w.client.GetHosts()
	if err != nil {
		w.checkError(err)
	}

	var usedResourcesByHost map[string]map[string]int64
	if !w.initialized {
		usedResourcesByHost, err = scheduler.GetUsedResourcesByHost(w.client)
		if err != nil {
			logrus.Panicf("Cannot get used resources for hosts. Error: %v", err)
		}
	}
	newKnownHosts := map[string]bool{}

	shouldSend := false
	dif := w.resourceUpdater.CompareHostLabels(hosts)
	if dif {
		shouldSend = true
	}

	for _, h := range hosts {
		newKnownHosts[h.UUID] = true
		delete(w.knownHosts, h.UUID)

		poolInits := map[string]int64{
			instancePool: totalAvailableInstances,
			cpuPool:      h.MilliCPU,
			memoryPool:   h.Memory,
			storageSize:  h.LocalStorageMb,
		}

		for resourceKey, total := range poolInits {
			// Update totals available, not amount used
			poolDoesntExist := !w.resourceUpdater.UpdateResourcePool(h.UUID, &scheduler.ComputeResourcePool{
				Resource: resourceKey,
				Total:    total,
			})
			if poolDoesntExist {
				if usedResourcesByHost == nil {
					usedResourcesByHost, err = scheduler.GetUsedResourcesByHost(w.client)
					if err != nil {
						logrus.Panicf("Cannot get used resources for hosts. Error: %v", err)
					}
				}

				usedResource := usedResourcesByHost[h.UUID][resourceKey]
				if err := w.resourceUpdater.CreateResourcePool(h.UUID, &scheduler.ComputeResourcePool{Resource: resourceKey, Total: total, Used: usedResource}); err != nil {
					logrus.Panicf("Received an error creating resource pool. This shouldn't have happened. Error: %v.", err)
				}
			}
		}

		// port pool update logic
		portPool, err := scheduler.GetPortPoolFromHost(h, w.client)
		if err != nil {
			w.checkError(err)
		}
		poolDoesntExist := !w.resourceUpdater.UpdateResourcePool(h.UUID, portPool)
		if poolDoesntExist {
			fmt.Print(h.UUID)
			w.resourceUpdater.CreateResourcePool(h.UUID, portPool)
		} else if w.previousIPs[h.UUID] != h.Labels[ipLabel] {
			portPool.ShouldUpdate = true
			w.resourceUpdater.UpdateResourcePool(h.UUID, portPool)
		}
		w.previousIPs[h.UUID] = h.Labels[ipLabel]

		// updating label pool
		labelPool := &scheduler.LabelPool{
			Resource: hostLabels,
			Labels:   h.Labels,
		}
		poolDoesntExist = !w.resourceUpdater.UpdateResourcePool(h.UUID, labelPool)
		if poolDoesntExist {
			w.resourceUpdater.CreateResourcePool(h.UUID, labelPool)
		}

	}

	for uuid := range w.knownHosts {
		w.resourceUpdater.RemoveHost(uuid)
	}

	// call reconcile hook after updating scheduler pool
	if shouldSend {
		err = sendExternalEvent(w.rclient)
		if err != nil {
			logrus.Warnf("Error in sending external host event. err: %+v", err)
		}
	}

	w.knownHosts = newKnownHosts
	w.initialized = true
}

func (w *metadataWatcher) checkError(err error) {
	w.consecutiveErrorCount++
	if w.consecutiveErrorCount > 5 {
		panic(fmt.Sprintf("%v consecutive errors attempting to reach metadata. Panicing. Error: %v", w.consecutiveErrorCount, err))
	}
	logrus.Errorf("Error %v getting metadata: %v", w.consecutiveErrorCount, err)
}

func sendExternalEvent(rclient *client.RancherClient) error {
	if rclient == nil {
		return nil
	}
	externalHostEvent := &client.ExternalHostEvent{
		EventType: schedulerUpdate,
	}
	if _, err := rclient.ExternalHostEvent.Create(externalHostEvent); err != nil {
		return err
	}
	return nil
}

type poolInitializer struct {
	total int64
}
