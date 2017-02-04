package scheduler

import (
	"math/rand"
	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
)

const (
	base         = 32768
	end          = 61000
	instanceID   = "instanceID"
	allocatedIPs = "allocatedIPs"
	allocatedIP  = "allocatedIP"
	publicPort   = "publicPort"
	privatePort  = "privatePort"
	protocol     = "protocol"
)

// ReserveIPPort reserve an ip and port from a port pool
// if the ip is not in the default map, then reserve on the ghost map.
// 	case 1: reserve default map 0.0.0.0, ip 192.168.1.1. Check if port is used by 0.0.0.0, if not then reserve on ghost map
//	case 2: reserve default map 192.168.1.1, 192.168.1.2, ip 192.168.1.3. Check if port is used by 0.0.0.0(ghost map), then reserve.
// 	case 3: ip 0.0.0.0, reserve every ip in ghost map and default map.
// if the ip is in the default map and it is 0.0.0.0, then do a check on ghost map to see if port is used
func (p *PortResourcePool) ReserveIPPort(ip string, port int64, protocol string, instanceUUID string) error {
	portMap := map[string]map[int64]string{}
	ghostMap := map[string]map[int64]string{}
	if protocol == "tcp" {
		portMap = p.PortBindingMapTCP
		ghostMap = p.GhostMapTCP
	} else {
		portMap = p.PortBindingMapUDP
		ghostMap = p.GhostMapUDP
	}
	if _, ok := portMap[ip]; !ok {
		// if ip can't be found and it is not 0.0.0.0,  reserve on the ghost map
		if ip != defaultIP {
			// before reserving on the ghost map, check 0.0.0.0 pool to make sure we cover this case:
			// Host Label only has 0.0.0.0, and container A use 0.0.0.0:8080:8080, container B use 192.168.1.1:8080:8080, should fail.
			if _, ok := p.PortBindingMapTCP[defaultIP]; ok {
				if portMap[defaultIP][port] != "" {
					return errors.Errorf("Can not reserve Port %v on IP %v, port is used by %v", port, ip, defaultIP)
				}
			}
			//Host Label 192.168.1.1, 192.168.1.2, Container A use 0.0.0.0:8080:8080(ghost map), container B use 192.168.1.3:8080:8080, should fail.
			if _, ok := p.GhostMapTCP[defaultIP]; ok {
				if ghostMap[defaultIP][port] != "" {
					return errors.Errorf("Can not reserve Port %v on IP %v, port is used by %v", port, ip, defaultIP)
				}
			}
			if _, ok := ghostMap[ip]; !ok {
				ghostMap[ip] = map[int64]string{}
				logrus.Infof("Creating ghost map for IP %v on protocol %v", ip, protocol)
				ghostMap[ip][port] = instanceUUID
				logrus.Infof("Port %v is reserved for IP %v for ghost map on protocol %v", port, ip, protocol)
				return nil
			}
			if ghostMap[ip][port] == "" {
				ghostMap[ip][port] = instanceUUID
				logrus.Infof("Port %v is reserved for IP %v for ghost map on protocol %v", port, ip, protocol)
				return nil
			}
			if instanceUUID == ghostMap[ip][port] {
				// the instance ID is equal to the id in the map, return nil
				return nil
			}
			return errors.Errorf("Port %v is already used in ip %v on protocol %v", port, ip, protocol)
		}
		// in this case, 0.0.0.0 is not on the pool so there must be multiple ip on the host
		// reserve all ips on the specified port
		success := true
		for key := range portMap {
			if portMap[key][port] == "" {
				portMap[key][port] = instanceUUID
				continue
			}
			if instanceUUID == portMap[ip][port] {
				continue
			}
			success = false
			break
		}
		for key := range ghostMap {
			if ghostMap[key][port] == "" {
				ghostMap[key][port] = instanceUUID
				continue
			}
			if instanceUUID == ghostMap[ip][port] {
				continue
			}
			success = false
			break
		}
		if success {
			return nil
		}
		return errors.New("The public ip address specified can't be found on the pool")
	}
	if portMap[ip][port] == "" {
		// if ip is 0.0.0.0, do a check on all ghost ip before reserving
		if ip == defaultIP {
			for gip, m := range ghostMap {
				if m[port] != "" {
					return errors.Errorf("Can not reserve Port %v on IP %v, Port is used by IP %v", port, ip, gip)
				}
			}
		}
		portMap[ip][port] = instanceUUID
		logrus.Infof("Port %v is reserved for IP %v on protocol %v", port, ip, protocol)
		return nil
	}
	if instanceUUID == portMap[ip][port] {
		return nil
	}
	return errors.Errorf("Port %v is already used in ip %v on protocol %v", port, ip, protocol)
}

func (p *PortResourcePool) ReleasePort(ip string, port int64, protocol string, uuid string) {
	portMap := map[string]map[int64]string{}
	ghostMap := map[string]map[int64]string{}
	if protocol == "tcp" {
		portMap = p.PortBindingMapTCP
		ghostMap = p.GhostMapTCP
	} else {
		portMap = p.PortBindingMapUDP
		ghostMap = p.GhostMapUDP
	}
	if _, ok := portMap[ip]; ok {
		if portMap[ip][port] == uuid || uuid == "" {
			delete(portMap[ip], port)
			logrus.Infof("Port %v is released on IP %v on protocol %v", port, ip, protocol)
		}
	} else if _, ok := ghostMap[ip]; ok {
		if ghostMap[ip][port] == uuid || uuid == "" {
			delete(ghostMap[ip], port)
			logrus.Infof("Port %v is released on IP %v on protocol %v", port, ip, protocol)
		}
	}
	if ip == defaultIP {
		// if ip is 0.0.0.0, also release all port on other pools
		for nip := range portMap {
			if portMap[nip][port] == uuid || uuid == "" {
				delete(portMap[nip], port)
				logrus.Infof("Port %v is released on IP %v on protocol %v", port, nip, protocol)
			}
		}
		for gip := range ghostMap {
			if ghostMap[gip][port] == uuid || uuid == "" {
				delete(ghostMap[gip], port)
				logrus.Infof("Port %v is released on IP %v on protocol %v", port, gip, protocol)
			}
		}
	}
}

func (p *PortResourcePool) ArePortsAvailable(ports []PortSpec) bool {
L:
	for ip, portMap := range p.PortBindingMapTCP {
		portMapTCP := portMap
		portMapUDP := p.PortBindingMapUDP[ip]
		if ip == defaultIP {
			// if ip is 0.0.0.0, do a check for all ips on the ghost map
			for _, port := range ports {
				if port.Protocol == "tcp" {
					for _, m := range p.GhostMapTCP {
						if m[port.PublicPort] != "" {
							return false
						}
					}
				} else {
					for _, m := range p.GhostMapUDP {
						if m[port.PublicPort] != "" {
							return false
						}
					}
				}
			}
		}
		for _, port := range ports {
			if port.Protocol == "tcp" {
				if port.IPAddress != "" {
					if ip != port.IPAddress || portMapTCP[port.PublicPort] != "" {
						continue L
					}
					continue
				}
				if portMapTCP[port.PublicPort] != "" {
					continue L
				}
			} else {
				if port.IPAddress != "" {
					if ip != port.IPAddress || portMapUDP[port.PublicPort] != "" {
						continue L
					}
					continue
				}
				if portMapUDP[port.PublicPort] != "" {
					continue L
				}
			}

		}
		return true
	}
	return false
}

func (p *PortResourcePool) IsIPQualifiedForRequests(ip, uuid string, specs []PortSpec) bool {
	qualified := true
	for _, spec := range specs {
		// iterate through all the requests, then check if port is used
		// if spec has an ip, then only check the port if ip is the same
		m := map[int64]string{}
		if spec.Protocol == "tcp" {
			m = p.PortBindingMapTCP[ip]
		} else {
			m = p.PortBindingMapUDP[ip]
		}
		if spec.IPAddress != "" {
			if spec.IPAddress == ip && m[spec.PublicPort] != "" && m[spec.PublicPort] != uuid {
				qualified = false
				break
			}
		} else {
			if m[spec.PublicPort] != "" && m[spec.PublicPort] != uuid {
				qualified = false
				break
			}
		}
	}
	return qualified
}

func PortReserve(pool *PortResourcePool, request PortBindingResourceRequest) (map[string]interface{}, error) {
	data := map[string]interface{}{}
	data[instanceID] = request.InstanceID
	portReservation := []map[string]interface{}{}
	found := false
	// since all the port requests should only land on one ip, loop through all the ip and find the available one
	for ip := range pool.PortBindingMapTCP {
		if pool.IsIPQualifiedForRequests(ip, request.ResourceUUID, request.PortRequests) {
			for _, spec := range request.PortRequests {
				if spec.IPAddress != "" {
					// if user has specified the public ip address, there is nothing we can do in the scheduler
					if err := pool.ReserveIPPort(spec.IPAddress, spec.PublicPort, spec.Protocol, request.ResourceUUID); err != nil {
						data[allocatedIPs] = portReservation
						return data, err
					}
					result := map[string]interface{}{}
					result[allocatedIP] = spec.IPAddress
					result[publicPort] = spec.PublicPort
					result[privatePort] = spec.PrivatePort
					result[protocol] = spec.Protocol
					portReservation = append(portReservation, result)
					continue
				}
				if spec.PublicPort != 0 {
					// if user has specified public port, find an ip for it
					err := pool.ReserveIPPort(ip, spec.PublicPort, spec.Protocol, request.ResourceUUID)
					if err != nil {
						data[allocatedIPs] = portReservation
						return data, err
					}
					result := map[string]interface{}{}
					result[allocatedIP] = ip
					result[publicPort] = spec.PublicPort
					result[privatePort] = spec.PrivatePort
					result[protocol] = spec.Protocol
					portReservation = append(portReservation, result)
					continue
				} else {
					// if user doesn't not specify the public port, scheduler will pick up an random port for them
					// find the random port
					// I don't believe the ports will get exhausted
					var portMap map[int64]string
					if spec.Protocol == "tcp" {
						portMap = pool.PortBindingMapTCP[ip]
					} else {
						portMap = pool.PortBindingMapUDP[ip]
					}
					port := findRandomPort(portMap)
					portMap[port] = request.ResourceUUID
					logrus.Infof("Public port %v reserved for ip address %s on protocol %v", port, ip, spec.Protocol)
					result := map[string]interface{}{}
					result[allocatedIP] = ip
					result[publicPort] = port
					result[privatePort] = spec.PrivatePort
					result[protocol] = spec.Protocol
					portReservation = append(portReservation, result)
					continue
				}
			}
			found = true
			break
		}
	}
	if !found {
		return data, errors.New("No available ip satisfies the port requests")
	}
	data[allocatedIPs] = portReservation
	return data, nil
}

func PortRelease(pool *PortResourcePool, request PortBindingResourceRequest) {
	for _, spec := range request.PortRequests {
		ip := spec.IPAddress
		if ip == "" {
			ip = defaultIP
		}
		pool.ReleasePort(ip, spec.PublicPort, spec.Protocol, request.ResourceUUID)
	}
}

func findRandomPort(portMap map[int64]string) int64 {
	// find a random port not used within the range of 32768--61000
	// algorithm is here http://stackoverflow.com/questions/6443176/how-can-i-generate-a-random-number-within-a-range-but-exclude-some though i can't prove it mathematically ^^
	s1 := rand.NewSource(time.Now().UnixNano())
	r1 := rand.New(s1)
	random := int64(base + r1.Intn(end-base+1-len(portMap)))
	for ex := range portMap {
		if random < ex {
			break
		}
		random++
	}
	return random
}
