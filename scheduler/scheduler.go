package scheduler

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

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

// ReservePort reserve a port from pool. Return allocated ip and true if port is reserved
func (p *PortResourcePool) ReservePort(port int64) (string, bool) {
	for ip, ports := range p.PortBindingMap {
		if used := ports[port]; !used {
			ports[port] = true
			return ip, true
		}
	}
	return "", false
}

// ReserveIPPort reserve an ip and port from a port pool
func (p *PortResourcePool) ReserveIPPort(ip string, port int64) error {
	if _, ok := p.PortBindingMap[ip]; !ok {
		// if ip can't be found,  reserve for the default ip 0.0.0.0
		if _, ok := p.PortBindingMap[defaultIP]; ok {
			p.PortBindingMap[defaultIP][port] = true
			return nil
		}
		// if 0.0.0.0 is not on the pool
		if ip == defaultIP {
			// in this case, 0.0.0.0 is not on the pool so there must be multiple ip on the host
			// reserve all ips on the specified port
			success := true
			for key := range p.PortBindingMap {
				if !p.PortBindingMap[key][port] {
					p.PortBindingMap[key][port] = true
					continue
				}
				success = false
				break
			}
			if success {
				return nil
			}
		}
		return errors.New("The public ip address specified can't be found on the pool")
	}
	if !p.PortBindingMap[ip][port] {
		p.PortBindingMap[ip][port] = true
		return nil
	}
	return errors.Errorf("Port %v is already used in ip %v", port, ip)
}

func (p *PortResourcePool) ReleasePort(ip string, port int64) {
	if portsMap, ok := p.PortBindingMap[ip]; ok {
		delete(portsMap, port)
	}
}

func (p *PortResourcePool) ArePortsAvailable(ports []PortSpec) bool {
	for _, portMap := range p.PortBindingMap {
		for _, port := range ports {
			if portMap[port.PublicPort] {
				break
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

func NewScheduler() *Scheduler {
	return &Scheduler{
		hosts: map[string]*host{},
	}
}

type Scheduler struct {
	mu    sync.RWMutex
	hosts map[string]*host
}

func (s *Scheduler) PrioritizeCandidates(resourceRequests []ResourceRequest) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

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
			request := rr.(ComputeResourceRequest)
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
			if er != nil {
				err = er
				break L
			} else {
				data[request.Resource] = result
			}
		}
	}

	if err == nil {
		logrus.Info(reserveLog.String())
	} else {
		// rollback
		// TODO We may need to add rollback logic for ports out here instead of in port.go
		for _, rr := range resourceRequests[:i] {
			p, ok := h.pools[rr.GetResourceType()]
			if !ok {
				break
			}
			resourcePoolType := p.GetPoolType()
			switch resourcePoolType {
			case computePool:
				pool := p.(*ComputeResourcePool)
				request := rr.(ComputeResourceRequest)
				pool.Used = pool.Used - request.Amount
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
			request := rr.(ComputeResourceRequest)
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
			logrus.Infof("Host-UUID %v, PortPool Map %v", hostID, pool.PortBindingMap)
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
		for ip := range p.PortBindingMap {
			ipset = append(ipset, ip)
		}
		logrus.Infof("Adding resource pool [%v], ip set [%v]", p.Resource, ipset)
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
