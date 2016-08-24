package resourcewatchers

import (
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/rancher/go-rancher-metadata/metadata"
	"github.com/rancher/scheduler/scheduler"
	"sync"
)

func WatchMetadata(client metadata.Client, updater scheduler.ResourceUpdater) {
	logrus.Infof("Subscribing to metadata changes.")

	watcher := &metadataWatcher{
		resourceUpdater: updater,
		client:          client,
		knownHosts:      map[string]bool{},
	}
	client.OnChange(5, watcher.updateFromMetadata)
}

type metadataWatcher struct {
	resourceUpdater       scheduler.ResourceUpdater
	client                metadata.Client
	consecutiveErrorCount int
	initialized           bool
	knownHosts            map[string]bool
	mu                    sync.Mutex
}

const memoryPool string = "memoryReservation"
const cpuPool string = "cpuReservation"
const storageSize string = "storageSize"

func (w *metadataWatcher) updateFromMetadata(mdVersion string) {
	w.mu.Lock()
	defer w.mu.Unlock()

	hosts, err := w.client.GetHosts()
	if err != nil {
		w.checkError(err)
	}

	var usedResourcesByHost map[string]map[string]int64
	if !w.initialized {
		usedResourcesByHost, err = w.getUsedResourcesByHost()
		if err != nil {
			logrus.Panicf("Cannot get used resources for hosts. Error: %v", err)
		}
	}
	newKnownHosts := map[string]bool{}

	for _, h := range hosts {
		newKnownHosts[h.UUID] = true
		delete(w.knownHosts, h.UUID)

		poolInits := map[string]int64{
			cpuPool:     h.MilliCPU,
			memoryPool:  h.Memory,
			storageSize: h.LocalStorageMb,
		}

		for resourceKey, total := range poolInits {
			poolDoesntExist := !w.resourceUpdater.UpdateResourcePool(h.UUID, resourceKey, total)
			if poolDoesntExist {
				if usedResourcesByHost == nil {
					usedResourcesByHost, err = w.getUsedResourcesByHost()
					if err != nil {
						logrus.Panicf("Cannot get used resources for hosts. Error: %v", err)
					}
				}

				usedResource := usedResourcesByHost[h.UUID][resourceKey]
				if err := w.resourceUpdater.CreateResourcePool(h.UUID, resourceKey, total, usedResource); err != nil {
					logrus.Panicf("Received an error creating resource pool. This shouldn't have happened. Error: %v.", err)
				}

			}
		}
	}

	for uuid := range w.knownHosts {
		w.resourceUpdater.RemoveHost(uuid)
	}

	w.knownHosts = newKnownHosts
	w.initialized = true
}

func (w *metadataWatcher) getUsedResourcesByHost() (map[string]map[string]int64, error) {
	resourcesByHost := map[string]map[string]int64{}
	containers, err := w.client.GetContainers()
	if err != nil {
		w.checkError(err)
	}

	for _, c := range containers {
		usedRes, ok := resourcesByHost[c.HostUUID]
		if !ok {
			usedRes = map[string]int64{}
			resourcesByHost[c.HostUUID] = usedRes
		}

		usedRes[memoryPool] += c.MemoryReservation
		usedRes[cpuPool] += c.MilliCPUReservation
	}

	return resourcesByHost, nil
}

func (w *metadataWatcher) checkError(err error) {
	w.consecutiveErrorCount++
	if w.consecutiveErrorCount > 5 {
		panic(fmt.Sprintf("%v consecutive errors attempting to reach metadata. Panicing. Error: %v", w.consecutiveErrorCount, err))
	}
	logrus.Errorf("Error %v getting metadata: %v", w.consecutiveErrorCount, err)
}

type poolInitializer struct {
	total int64
}
