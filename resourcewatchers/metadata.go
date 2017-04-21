package resourcewatchers

import (
	"fmt"
	"sync"

	"strconv"
	"strings"

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
	portPool                string = "portReservation"
	hostLabels              string = "hostLabels"
	ipLabel                 string = "io.rancher.scheduler.ips"
	defaultIP               string = "0.0.0.0"
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
		usedResourcesByHost, err = w.getUsedResourcesByHost()
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
		portPool := w.getPortPoolFromHost(h)
		// Note: UpdateResourcePool for ports is effectively a no-op. It just checks if the pool has been created.
		// This means that we cannot currently back-populate "native" containers into the scheduler.
		poolDoesntExist := !w.resourceUpdater.UpdateResourcePool(h.UUID, portPool)
		if poolDoesntExist {
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

	if !w.initialized {
		// update service
		services, err := w.client.GetServices()
		if err != nil {
			logrus.Errorf("Error in getting service froms metadata; err: %v", err)
		}
		w.resourceUpdater.UpdateService(services)
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

func (w *metadataWatcher) getPortPoolFromHost(h metadata.Host) *scheduler.PortResourcePool {
	pool := &scheduler.PortResourcePool{
		PortBindingMapTCP: map[string]map[int64]string{},
		GhostMapTCP:       map[string]map[int64]string{},
		PortBindingMapUDP: map[string]map[int64]string{},
		GhostMapUDP:       map[string]map[int64]string{},
	}
	pool.Resource = portPool
	label := h.Labels[ipLabel]
	if label == "" {
		// only one ip, set ip as 0.0.0.0
		pool.PortBindingMapTCP[defaultIP] = map[int64]string{}
		pool.PortBindingMapUDP[defaultIP] = map[int64]string{}
	} else {
		ips := strings.Split(label, ",")
		for _, ip := range ips {
			pool.PortBindingMapTCP[strings.TrimSpace(ip)] = map[int64]string{}
			pool.PortBindingMapUDP[strings.TrimSpace(ip)] = map[int64]string{}
		}
	}
	containers, err := w.client.GetContainers()
	if err != nil {
		w.checkError(err)
	}
	for _, container := range containers {
		if container.HostUUID == h.UUID && container.State == "running" {
			for _, portString := range container.Ports {
				if ip, port, proto, ok := parsePort(portString); ok {
					if proto == "tcp" {
						setPortBinding(pool.PortBindingMapTCP, pool.GhostMapTCP, ip, port, container)
					} else {
						setPortBinding(pool.PortBindingMapUDP, pool.GhostMapUDP, ip, port, container)
					}
				}
			}
		}
	}
	return pool
}

func setPortBinding(bindings map[string]map[int64]string, ghostBindings map[string]map[int64]string, ip string,
	port int64, container metadata.Container) {
	if _, ok := bindings[ip]; ok {
		bindings[ip][port] = container.UUID
	} else if ip == defaultIP {
		for ip := range bindings {
			bindings[ip][port] = container.UUID
		}
	} else {
		if _, ok := ghostBindings[ip]; !ok {
			ghostBindings[ip] = map[int64]string{}
		}
		ghostBindings[ip][port] = container.UUID
	}
}

// expect ip:public:private, return ip and public
func parsePort(port string) (string, int64, string, bool) {
	// TODO look at how it is in cattle. We changed logic to make sure it always matches this format
	parts := strings.Split(port, ":")
	if len(parts) == 3 {
		publicPort, err := strconv.Atoi(parts[1])
		if err != nil {
			return "", 0, "", false
		}
		ip := parts[0]
		privateAndProt := parts[2]
		parts := strings.Split(privateAndProt, "/")
		if len(parts) == 2 {
			return ip, int64(publicPort), parts[1], true
		}
		return "", 0, "", false
	}
	return "", 0, "", false
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
