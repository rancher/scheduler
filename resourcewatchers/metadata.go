package resourcewatchers

import (
	"fmt"
	"sync"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/go-rancher-metadata/metadata"
	"github.com/rancher/scheduler/scheduler"
	"strconv"
	"strings"
)

func WatchMetadata(client metadata.Client, updater scheduler.ResourceUpdater) error {
	logrus.Infof("Subscribing to metadata changes.")

	watcher := &metadataWatcher{
		resourceUpdater: updater,
		client:          client,
		knownHosts:      map[string]bool{},
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
}

const (
	instancePool            string = "instanceReservation"
	memoryPool              string = "memoryReservation"
	cpuPool                 string = "cpuReservation"
	storageSize             string = "storageSize"
	totalAvailableInstances int64  = 1000000
	portPool                string = "portReservation"
	ipLabel                 string = "io.rancher.scheduler.ips"
	defaultIP               string = "0.0.0.0"
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
			instancePool: totalAvailableInstances,
			cpuPool:      h.MilliCPU,
			memoryPool:   h.Memory,
			storageSize:  h.LocalStorageMb,
		}

		for resourceKey, total := range poolInits {
			poolDoesntExist := !w.resourceUpdater.UpdateResourcePool(h.UUID, &scheduler.ComputeResourcePool{
				Resource: resourceKey,
				Total:    total,
			})
			if poolDoesntExist {
				if usedResourcesByHost == nil {
					usedResourcesByHost, err = w.getUsedResourcesByHost()
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
		portPool := w.GetPortPoolFromHost(h)
		logrus.Infof("Updating PortPool from metadata. Host-UUID: %v, Port Map: %v", h.UUID, portPool.(*scheduler.PortResourcePool).PortBindingMap)
		poolDoesntExist := !w.resourceUpdater.UpdateResourcePool(h.UUID, portPool)
		if poolDoesntExist {
			w.resourceUpdater.CreateResourcePool(h.UUID, portPool)
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

		usedRes[instancePool]++
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

// the correctness of the port-reserve logic is highly relied on the correctness information that metadata provides
// The method goes into the host and iterate the running containers. The port reported by metadata will get populated into the map
// TODO: the port info is not complete as some of them are missing the public port info(native container running through docker cli)
func (w *metadataWatcher) GetPortPoolFromHost(h metadata.Host) scheduler.ResourcePool {
	pool := &scheduler.PortResourcePool{PortBindingMap: map[string]map[int64]bool{}}
	pool.Resource = portPool
	label := h.Labels[ipLabel]
	if label == "" {
		// only one ip, set ip as 0.0.0.0
		portUsed := map[int64]bool{}
		containers, err := w.client.GetContainers()
		if err != nil {
			w.checkError(err)
		}
		for _, container := range containers {
			if container.State == "running" && container.HostUUID == h.UUID {
				for _, portString := range container.Ports {
					_, port, ok := parsePort(portString)
					if ok {
						portUsed[port] = true
					}
				}
			}
		}
		pool.PortBindingMap[defaultIP] = portUsed
	} else {
		ips := strings.Split(label, ",")
		for _, ip := range ips {
			pool.PortBindingMap[strings.Trim(ip, " ")] = map[int64]bool{}
		}
		containers, err := w.client.GetContainers()
		if err != nil {
			w.checkError(err)
		}
		for _, container := range containers {
			if container.State == "running" && container.HostUUID == h.UUID {
				for _, portString := range container.Ports {
					ip, port, ok := parsePort(portString)
					if ok {
						if _, ok := pool.PortBindingMap[ip]; ok {
							pool.PortBindingMap[ip][port] = true
						} else if ip == defaultIP {
							for ip := range pool.PortBindingMap {
								pool.PortBindingMap[ip][port] = true
							}
						}
					}
				}
			}
		}
	}
	return pool
}

// expect ip:public:private, return ip and public
func parsePort(port string) (string, int64, bool) {
	parts := strings.Split(port, ":")
	if len(parts) == 3 {
		publicPort, err := strconv.Atoi(parts[1])
		if err != nil {
			return "", 0, false
		}
		ip := parts[0]
		return ip, int64(publicPort), true
	}
	return "", 0, false
}

type poolInitializer struct {
	total int64
}
