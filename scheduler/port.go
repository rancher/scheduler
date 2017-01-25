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

// PortReserve reserve a port from portPool
// {
//      "instanceID" : "xx",
//      "allocatedIPs" : {
//                             [
//                                  {
//                                      "allocatedIP" : "xxx.xxx.xxx.xxx",
//                                      "publicPort" : "xxx",
//                                      "privatePort" :  "xxx",
//                                  },
//                                  {
//                                      "allocatedIP" : "xxx.xxx.xxx.xxx",
//                                      "publicPort" : "xxx",
//                                      "privatePort" :  "xxx",
//                                  }
//                              ]
//                         }
// }
func PortReserve(pool *PortResourcePool, request PortBindingResourceRequest) (map[string]interface{}, error) {
	data := map[string]interface{}{}
	data[instanceID] = request.InstanceID
	portReservation := []map[string]interface{}{}
	found := false
	// since all the port requests should only land on one ip, loop through all the ip and find the available one
	for ip := range pool.PortBindingMapTCP {
		if pool.IsIPQualifiedForRequests(ip, request.PortRequests) {
			for _, spec := range request.PortRequests {
				if spec.IPAddress != "" {
					// if user has specified the public ip address, there is nothing to do we can do in the scheduler
					if err := pool.ReserveIPPort(spec.IPAddress, spec.PublicPort, spec.Protocol); err != nil {
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
					err := pool.ReserveIPPort(ip, spec.PublicPort, spec.Protocol)
					if err != nil {
						data[allocatedIPs] = portReservation
						return data, err
					}
					logrus.Infof("Public port %v reserved for ip address %v on protocol %v", spec.PublicPort, ip, spec.Protocol)
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
					portMap := map[int64]bool{}
					if spec.Protocol == "tcp" {
						portMap = pool.PortBindingMapTCP[ip]
					} else {
						portMap = pool.PortBindingMapUDP[ip]
					}
					port := findRandomPort(portMap)
					portMap[port] = true
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
		pool.ReleasePort(spec.IPAddress, spec.PublicPort, spec.Protocol)
	}
}

func findRandomPort(portMap map[int64]bool) int64 {
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
