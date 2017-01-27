package scheduler

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	"time"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
)

/*
	TODO: debate on poolName
*/

const (
	computePool = "computePool"
	portPool    = "portPool"
	defaultIP   = "0.0.0.0"
)

func (p *PortResourcePool) IsIPQualifiedForRequests(ip string, specs []PortSpec) bool {
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
			if spec.IPAddress == ip && m[spec.PublicPort] != "" {
				qualified = false
				break
			}
		} else {
			if m[spec.PublicPort] != "" {
				qualified = false
				break
			}
		}
	}
	return qualified
}

// ReserveIPPort reserve an ip and port from a port pool
func (p *PortResourcePool) ReserveIPPort(ip string, port int64, protocol string, phase string, instanceUUID string) error {
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
			// check if the phase is instance.start and the instance is allocated
			if phase == "instance.start" {
				if instanceUUID == ghostMap[ip][port] {
					// the instance ID is equal to the id in the map, return nil
					return nil
				}
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
			if phase == "instance.start" {
				if instanceUUID == portMap[ip][port] {
					continue
				}
			}
			success = false
			break
		}
		for key := range ghostMap {
			if ghostMap[key][port] == "" {
				ghostMap[key][port] = instanceUUID
				continue
			}
			if phase == "instance.start" {
				if instanceUUID == ghostMap[ip][port] {
					continue
				}
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
		portMap[ip][port] = instanceUUID
		logrus.Infof("Port %v is reserved for IP %v on protocol %v", port, ip, protocol)
		return nil
	}
	if phase == "instance.start" {
		if instanceUUID == portMap[ip][port] {
			return nil
		}
	}
	return errors.Errorf("Port %v is already used in ip %v on protocol %v", port, ip, protocol)
}

func (p *PortResourcePool) ReleasePort(ip string, port int64, protocol string) {
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
		delete(portMap[ip], port)
		logrus.Infof("Port %v is released on IP %v on protocol %v", port, ip, protocol)
	} else if _, ok := ghostMap[ip]; ok {
		delete(ghostMap[ip], port)
		logrus.Infof("Port %v is released on IP %v for ghost map on protocol %v", port, ip, protocol)
	}
}

func (p *PortResourcePool) ArePortsAvailable(ports []PortSpec) bool {
L:
	for ip, portMap := range p.PortBindingMapTCP {
		portMapTCP := portMap
		portMapUDP := p.PortBindingMapUDP[ip]
		for _, port := range ports {
			if port.Protocol == "tcp" {
				if portMapTCP[port.PublicPort] != "" {
					continue L
				}
			} else {
				if portMapUDP[port.PublicPort] != "" {
					continue L
				}
			}

		}
		return true
	}
	return false
}

type host struct {
	id    string
	pools map[string]ResourcePool
}

func NewScheduler(sleepTime int) *Scheduler {
	return &Scheduler{
		hosts:     map[string]*host{},
		sleepTime: sleepTime,
	}
}

type Scheduler struct {
	mu        sync.RWMutex
	hosts     map[string]*host
	sleepTime int
}

func (s *Scheduler) PrioritizeCandidates(resourceRequests []ResourceRequest) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	hosts := filter(s.hosts, resourceRequests)

	if len(hosts) == 0 {
		return []string{}, nil
	}

	hs := hostSorter{
		hosts:            hosts,
		resourceRequests: resourceRequests,
	}
	sort.Sort(hs)
	sortedIDs := ids(hs.hosts)
	filteredHosts := s.PortFilter(resourceRequests, sortedIDs)
	s.reserveTempPool(sortedIDs[0], resourceRequests)
	return filteredHosts, nil
}

func (s *Scheduler) ReserveResources(hostID string, force bool, resourceRequests []ResourceRequest) (map[string]interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	logrus.Infof("Reserving %+v for %v. Force=%v", resourceRequests, hostID, force)
	h, ok := s.hosts[hostID]
	if !ok {
		// If the host isn't present, it is most likely that it hasn't been registered with the scheduler yet.
		// When it is, this reservation will get counted by the initial population.
		logrus.Warnf("Host %v not found for reserving %v. Skipping reservation", hostID, resourceRequests)
		return nil, nil
	}

	i := 0
	var err error
	data := map[string]interface{}{}
	portsRollback := []map[string]interface{}{}
	reserveLog := bytes.NewBufferString(fmt.Sprintf("New pool amount on host %v:", hostID))
L:
	for _, rr := range resourceRequests {
		p, ok := h.pools[rr.GetResourceType()]
		if !ok {
			logrus.Warnf("Pool %v for host %v not found for reserving %v. Skipping reservation", rr.GetResourceType(), hostID, rr)
			continue
		}
		PoolType := p.GetPoolType()
		switch PoolType {
		case computePool:
			pool := p.(*ComputeResourcePool)
			request := rr.(AmountBasedResourceRequest)
			if !force && pool.Used+request.Amount > pool.Total {
				err = OverReserveError{hostID: hostID, resourceRequest: rr}
				break L
			}

			pool.Used = pool.Used + request.Amount
			i++
			reserveLog.WriteString(fmt.Sprintf(" %v total: %v used: %v.", request.Resource, pool.Total, pool.Used))
		case portPool:
			pool := p.(*PortResourcePool)
			request := rr.(PortBindingResourceRequest)
			result, er := PortReserve(pool, request)
			logrus.Infof("Host-UUID %v, PortPool Map tcp %v, PortPool Map udp %v, Ghost Map tcp %v, Ghost Map udp %v", hostID, pool.PortBindingMapTCP, pool.PortBindingMapUDP, pool.GhostMapTCP, pool.GhostMapUDP)
			if er != nil {
				err = er
				portsRollback = append(portsRollback, result)
				break L
			} else {
				if _, ok := data[request.Resource]; !ok {
					data[request.Resource] = []map[string]interface{}{}
				}
				data[request.Resource] = append(data[request.Resource].([]map[string]interface{}), result)
			}
		}
	}

	if err == nil {
		logrus.Info(reserveLog.String())
	} else {
		logrus.Error(err)
		// rollback
		for _, rr := range resourceRequests[:i] {
			p, ok := h.pools[rr.GetResourceType()]
			if !ok {
				break
			}
			resourcePoolType := p.GetPoolType()
			switch resourcePoolType {
			case computePool:
				pool := p.(*ComputeResourcePool)
				request := rr.(AmountBasedResourceRequest)
				pool.Used = pool.Used - request.Amount
			}
		}
		// roll back ports
		if pool, ok := h.pools["portReservation"].(*PortResourcePool); ok {
			for _, prb := range portsRollback {
				if portReservation, ok := prb[allocatedIPs].([]map[string]interface{}); ok {
					for _, portReserved := range portReservation {
						ip := portReserved[allocatedIP].(string)
						port := portReserved[publicPort].(int64)
						prot := portReserved[protocol].(string)
						pool.ReleasePort(ip, port, prot)
						logrus.Infof("Roll back ip [%v] and port [%v]", ip, port)
					}
				}
			}
		}
		return nil, err
	}

	return data, nil
}

func (s *Scheduler) ReleaseResources(hostID string, resourceRequests []ResourceRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	logrus.Infof("Releasing %+v for %v", resourceRequests, hostID)
	h, ok := s.hosts[hostID]
	if !ok {
		logrus.Infof("Host %v not found for releasing %v. Nothing to do.", hostID, resourceRequests)
		return nil
	}

	releaseLog := bytes.NewBufferString(fmt.Sprintf("New pool amounts on host %v:", hostID))
	for _, rr := range resourceRequests {
		p, ok := h.pools[rr.GetResourceType()]
		if !ok {
			logrus.Infof("Host %v doesn't have resource pool %v. Nothing to do.", hostID, rr.GetResourceType())
			continue
		}
		PoolType := p.GetPoolType()
		switch PoolType {
		case computePool:
			pool := p.(*ComputeResourcePool)
			request := rr.(AmountBasedResourceRequest)
			if pool.Used-request.Amount < 0 {
				logrus.Infof("Decreasing used for %v.%v by %v would result in negative usage. Setting to 0.", hostID, request.Resource, request.Amount)
				pool.Used = 0
			} else {
				pool.Used = pool.Used - request.Amount
			}
			releaseLog.WriteString(fmt.Sprintf(" %v total: %v used: %v.", request.Resource, pool.Total, pool.Used))
		case portPool:
			pool := p.(*PortResourcePool)
			request := rr.(PortBindingResourceRequest)
			PortRelease(pool, request)
			logrus.Infof("Host-UUID %v, PortPool Map tcp %v, PortPool Map udp %v, Ghost Map tcp %v, Ghost Map udp %v", hostID, pool.PortBindingMapTCP, pool.PortBindingMapUDP, pool.GhostMapTCP, pool.GhostMapUDP)
		}

	}
	logrus.Info(releaseLog.String())
	return nil
}

func (s *Scheduler) CreateResourcePool(hostUUID string, pool ResourcePool) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	h, ok := s.hosts[hostUUID]
	if !ok {
		h = &host{
			pools: map[string]ResourcePool{},
			id:    hostUUID,
		}
		s.hosts[hostUUID] = h
	}

	if _, ok := h.pools[pool.GetPoolResourceType()]; ok {
		return fmt.Errorf("Pool %v already exists on host %v", pool.GetPoolResourceType(), hostUUID)
	}
	switch pool.GetPoolType() {
	case computePool:
		p := pool.(*ComputeResourcePool)
		logrus.Infof("Adding resource pool [%v] with total %v and used %v for host %v", p.Resource, p.Total, p.Used, hostUUID)
		h.pools[p.Resource] = &ComputeResourcePool{Total: p.Total, Used: p.Used, Resource: p.Resource}
	case portPool:
		p := pool.(*PortResourcePool)
		ipset := []string{}
		for ip := range p.PortBindingMapTCP {
			ipset = append(ipset, ip)
		}
		logrus.Infof("Adding resource pool [%v], ip set [%v], ports map tcp [%v], ports map udp [%v]", p.Resource, ipset, p.PortBindingMapTCP, p.PortBindingMapUDP)
		h.pools[p.Resource] = p
	}

	return nil
}

func (s *Scheduler) UpdateResourcePool(hostUUID string, pool ResourcePool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	h, ok := s.hosts[hostUUID]
	if !ok {
		return false
	}

	existingPool, ok := h.pools[pool.GetPoolResourceType()]
	if !ok {
		return false
	}
	poolType := existingPool.GetPoolType()
	switch poolType {
	case computePool:
		e := existingPool.(*ComputeResourcePool)
		p := pool.(*ComputeResourcePool)
		if e.Total != p.Total {
			logrus.Infof("Updating resource pool [%v] to %v for host %v", p.GetPoolResourceType(), p.Total, hostUUID)
			e.Total = p.Total
		}
	}

	return true
}

func (s *Scheduler) RemoveHost(hostUUID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	logrus.Infof("Removing host %v.", hostUUID)
	delete(s.hosts, hostUUID)
}

func (s *Scheduler) PortFilter(requests []ResourceRequest, hosts []string) []string {
	filteredHosts := []string{}
	for _, host := range hosts {
		qualified := true
		if portPool, ok := s.hosts[host].pools["portReservation"].(*PortResourcePool); ok {
			for _, request := range requests {
				if rr, ok := request.(PortBindingResourceRequest); ok {
					if !portPool.ArePortsAvailable(rr.PortRequests) {
						qualified = false
						break
					}
				}
			}
		}
		if qualified {
			filteredHosts = append(filteredHosts, host)
		}
	}
	return filteredHosts
}

type OverReserveError struct {
	hostID          string
	resourceRequest ResourceRequest
}

func (e OverReserveError) Error() string {
	return fmt.Sprintf("Not enough available resources on host %v to reserve %v.", e.hostID, e.resourceRequest)
}

func (s *Scheduler) reserveTempPool(hostID string, requests []ResourceRequest) {
	if s.sleepTime >= 0 {
		for _, rr := range requests {
			if computeReq, ok := rr.(AmountBasedResourceRequest); ok {
				pool := s.hosts[hostID].pools[computeReq.Resource].(*ComputeResourcePool)
				pool.Used += computeReq.Amount
				go func(amount int64, t int) {
					time.Sleep(time.Second * time.Duration(t))
					s.mu.Lock()
					pool.Used -= amount
					s.mu.Unlock()
				}(computeReq.Amount, s.sleepTime)
			}
		}
	}
}
