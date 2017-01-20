package scheduler

import (
	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"math/rand"
	"time"
)

const (
	base         = 32768
	end          = 61000
	instanceID   = "instanceID"
	allocatedIPs = "allocatedIPs"
	allocatedIP  = "allocatedIP"
	publicPort   = "publicPort"
	privatePort  = "privatePort"
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
	flagErr := false
	portReservation := []map[string]interface{}{}
	for _, spec := range request.PortRequests {
		if spec.IPAddress != "" {
			// if user has specified the public ip address, there is nothing to do we can do in the scheduler
			if err := pool.ReserveIPPort(spec.IPAddress, spec.PublicPort); err != nil {
				return nil, err
			}
			continue
		}
		if spec.PublicPort != 0 {
			// if user has specified public port, find an ip for it
			ip, success := pool.ReservePort(spec.PublicPort)
			if success {
				logrus.Infof("Public port %v reserved for ip address %v", spec.PublicPort, ip)
				result := map[string]interface{}{}
				result[allocatedIP] = ip
				result[publicPort] = spec.PublicPort
				result[privatePort] = spec.PrivatePort
				portReservation = append(portReservation, result)
				continue
			} else {
				logrus.Errorf("Port %v is already used", spec.PublicPort)
				flagErr = true
				break
			}

		} else {
			// if user doesn't not specify the public port, scheduler will pick up an random port for them
			// find the random port
			// I don't believe the ports will get exhausted
			for ip, portMap := range pool.PortBindingMap {
				port := findRandomPort(portMap)
				portMap[port] = true
				logrus.Infof("Public port %v reserved for ip address %s", port, ip)
				result := map[string]interface{}{}
				result[allocatedIP] = ip
				result[publicPort] = port
				result[privatePort] = spec.PrivatePort
				portReservation = append(portReservation, result)
				break
			}
		}
	}
	if flagErr {
		// if error happens, roll back
		for _, portReserved := range portReservation {
			ip := portReserved[allocatedIP].(string)
			port := portReserved[publicPort].(int64)
			pool.ReleasePort(ip, port)
			logrus.Infof("Roll back ip [%v] and port [%v]", ip, port)
		}
		return nil, errors.Errorf("Can not reserve port request for instance id %v", request.InstanceID)
	}
	data[allocatedIPs] = portReservation
	return data, nil
}

func PortRelease(pool *PortResourcePool, request PortBindingResourceRequest) {
	for _, spec := range request.PortRequests {
		pool.ReleasePort(spec.IPAddress, spec.PublicPort)
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
