package scheduler

import (
	"fmt"
	"sort"
	"sync"

	"github.com/Sirupsen/logrus"
)

type ResourceUpdater interface {
	CreateResourcePool(hostUUID, resource string, total, used int64) error
	UpdateResourcePool(hostUUID, resource string, total int64) bool
	RemoveHost(hostUUID string)
}

type ResourceRequest struct {
	Resource string
	Amount   int64
}

type resourcePool struct {
	resource string
	total    int64
	used     int64
}

type host struct {
	id    string
	pools map[string]*resourcePool
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
	return sortedIDs, nil
}

func (s *Scheduler) ReserveResources(hostID string, force bool, resourceRequests []ResourceRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	logrus.Infof("Reserving %+v for %v", resourceRequests, hostID)
	h, ok := s.hosts[hostID]
	if !ok {
		return fmt.Errorf("Could't find host %v", hostID)
	}

	var err error
	i := 0
	for _, rr := range resourceRequests {
		p, ok := h.pools[rr.Resource]
		if !ok {
			err = fmt.Errorf("Host %v doesn't have resource pool %v.", hostID, rr.Resource)
			break
		}

		if !force && p.used+rr.Amount > p.total {
			err = OverReserveError{hostID: hostID, resourceRequest: rr}
			break
		}

		p.used = p.used + rr.Amount
		i++
	}

	if err != nil {
		// rollback
		for _, rr := range resourceRequests[:i] {
			p, ok := h.pools[rr.Resource]
			if !ok {
				break
			}
			p.used = p.used - rr.Amount
		}
		return err
	}

	return nil
}

func (s *Scheduler) ReleaseResources(hostID string, resourceRequests []ResourceRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	logrus.Infof("Releasing %+v for %v", resourceRequests, hostID)
	h, ok := s.hosts[hostID]
	if !ok {
		return fmt.Errorf("Could't find host %v", hostID)
	}

	var err error
	for _, rr := range resourceRequests {
		p, ok := h.pools[rr.Resource]
		if !ok {
			err = fmt.Errorf("Host %v doesn't have resource pool %v.", hostID, rr.Resource)
			break
		}

		if p.used-rr.Amount < 0 {
			err = OverReleaseError{hostID: hostID, resourceRequest: rr}
			break
		}

		p.used = p.used - rr.Amount
	}

	if err != nil {
		// rollback
		for _, rr := range resourceRequests {
			p, ok := h.pools[rr.Resource]
			if !ok {
				break
			}
			p.used = p.used + rr.Amount
		}
		return err
	}

	return nil
}

func (s *Scheduler) CreateResourcePool(hostUUID, resource string, total, used int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	h, ok := s.hosts[hostUUID]
	if !ok {
		h = &host{
			pools: map[string]*resourcePool{},
			id:    hostUUID,
		}
		s.hosts[hostUUID] = h
	}

	if _, ok := h.pools[resource]; ok {
		return fmt.Errorf("Pool %v already exists on host %v.", resource, hostUUID)
	}

	logrus.Infof("Adding resource pool [%v] with total %v and used %v for host %v", resource, total, used, hostUUID)
	h.pools[resource] = &resourcePool{total: total, used: used, resource: resource}
	return nil
}

func (s *Scheduler) UpdateResourcePool(hostUUID, resource string, total int64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	h, ok := s.hosts[hostUUID]
	if !ok {
		return false
	}

	existingPool, ok := h.pools[resource]
	if !ok {
		return false
	}

	if existingPool.total != total {
		logrus.Infof("Updating resource pool [%v] to %v for host %v", resource, total, hostUUID)
		existingPool.total = total
	}

	return true
}

func (s *Scheduler) RemoveHost(hostUUID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	logrus.Infof("Removing host %v.", hostUUID)
	delete(s.hosts, hostUUID)
}

type OverReserveError struct {
	hostID          string
	resourceRequest ResourceRequest
}

func (e OverReserveError) Error() string {
	return fmt.Sprintf("Not enough available resources on host %v to reserve %v.", e.hostID, e.resourceRequest)
}

type OverReleaseError struct {
	hostID          string
	resourceRequest ResourceRequest
}

func (e OverReleaseError) Error() string {
	return fmt.Sprintf("Releasing %v on host %v would cause the resource pool to have negative usage.", e.resourceRequest, e.hostID)
}
