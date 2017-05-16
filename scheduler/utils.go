package scheduler

import (
	"strconv"
	"strings"

	"github.com/rancher/go-rancher-metadata/metadata"
)

func GetPortPoolFromHost(h metadata.Host, client metadata.Client) (*PortResourcePool, error) {
	pool := &PortResourcePool{
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
	containers, err := client.GetContainers()
	if err != nil {
		return nil, err
	}
	for _, container := range containers {
		if container.HostUUID == h.UUID && container.State == "running" {
			for _, portString := range container.Ports {
				if ip, port, proto, ok := ParsePort(portString); ok {
					if proto == "tcp" {
						SetPortBinding(pool.PortBindingMapTCP, pool.GhostMapTCP, ip, port, container)
					} else {
						SetPortBinding(pool.PortBindingMapUDP, pool.GhostMapUDP, ip, port, container)
					}
				}
			}
		}
	}
	return pool, nil
}

func SetPortBinding(bindings map[string]map[int64]string, ghostBindings map[string]map[int64]string, ip string,
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

// ParsePort parse port info in a string format. Expect ip:public:private, return ip and public
func ParsePort(port string) (string, int64, string, bool) {
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

func GetUsedResourcesByHost(client metadata.Client) (map[string]map[string]int64, error) {
	resourcesByHost := map[string]map[string]int64{}
	containers, err := client.GetContainers()
	if err != nil {
		return nil, err
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
