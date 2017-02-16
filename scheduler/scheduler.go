package scheduler

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	"time"

	"github.com/Sirupsen/logrus"
)

const (
	computePool = "computePool"
	portPool    = "portPool"
	labelPool   = "labelPool"
	defaultIP   = "0.0.0.0"
)

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

func (s *Scheduler) PrioritizeCandidates(resourceRequests []ResourceRequest, context Context) ([]string, error) {
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
	filteredHosts = s.LabelFilter(filteredHosts, context)
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
	var reserveLog *bytes.Buffer
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
			if reserveLog == nil {
				reserveLog = bytes.NewBufferString(fmt.Sprintf("New pool amount on host %v:", hostID))
			}
			reserveLog.WriteString(fmt.Sprintf(" %v total: %v used: %v.", request.Resource, pool.Total, pool.Used))
		case portPool:
			pool := p.(*PortResourcePool)
			request := rr.(PortBindingResourceRequest)
			result, e := PortReserve(pool, request)
			portsRollback = append(portsRollback, result)
			logrus.Debugf("Host-UUID %v, PortPool Map tcp %v, PortPool Map udp %v, Ghost Map tcp %v, Ghost Map udp %v", hostID, pool.PortBindingMapTCP, pool.PortBindingMapUDP, pool.GhostMapTCP, pool.GhostMapUDP)
			if e != nil && !force {
				err = e
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
		if reserveLog != nil {
			logrus.Info(reserveLog.String())
		}
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
						pool.ReleasePort(ip, port, prot, "")
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
		logrus.Infof("Adding resource pool [%v] with total %v and used %v for host  %v", p.Resource, p.Total, p.Used, hostUUID)
		h.pools[p.Resource] = &ComputeResourcePool{Total: p.Total, Used: p.Used, Resource: p.Resource}
	case portPool:
		p := pool.(*PortResourcePool)
		ipset := []string{}
		for ip := range p.PortBindingMapTCP {
			ipset = append(ipset, ip)
		}
		logrus.Infof("Adding resource pool [%v], ip set %v, ports map tcp %v, ports map udp %v for host %v", p.Resource,
			ipset, p.PortBindingMapTCP, p.PortBindingMapUDP, hostUUID)
		h.pools[p.Resource] = p
	case labelPool:
		p := pool.(*LabelPool)
		logrus.Infof("Adding resource pool [%v] with label map [%v]", p.Resource, p.Labels)
		h.pools[p.Resource] = p
	}

	return nil
}

func (s *Scheduler) UpdateResourcePool(hostUUID string, pool ResourcePool, updatePool bool) bool {
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
	case portPool:
		if updatePool {
			p := pool.(*PortResourcePool)
			ipset := []string{}
			for ip := range p.PortBindingMapTCP {
				ipset = append(ipset, ip)
			}
			logrus.Infof("Adding resource pool [%v], ip set %v, ports map tcp %v, ports map udp %v for host %v", p.Resource,
				ipset, p.PortBindingMapTCP, p.PortBindingMapUDP, hostUUID)
			h.pools[p.Resource] = p
		}
	case labelPool:
		p := pool.(*LabelPool)
		h.pools[p.Resource] = p
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
		portPool, ok := s.hosts[host].pools["portReservation"].(*PortResourcePool)
		if !ok {
			logrus.Warnf("Pool portReservation for host %v not found for reserving %v. Skipping pritization", hosts)
		}
		qualified := true
		for _, request := range requests {
			if rr, ok := request.(PortBindingResourceRequest); ok {
				if !portPool.ArePortsAvailable(rr.PortRequests) {
					qualified = false
					break
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
