package scheduler

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	"github.com/Sirupsen/logrus"
	"time"
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
	s.reserveTempPool(sortedIDs[0], resourceRequests)
	return sortedIDs, nil
}

func (s *Scheduler) ReserveResources(hostID string, force bool, resourceRequests []ResourceRequest) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	logrus.Infof("Reserving %+v for %v. Force=%v", resourceRequests, hostID, force)
	h, ok := s.hosts[hostID]
	if !ok {
		// If the host isn't present, it is most likely that it hasn't been registered with the scheduler yet.
		// When it is, this reservation will get counted by the initial population.
		logrus.Warnf("Host %v not found for reserving %v. Skipping reservation", hostID, resourceRequests)
		return nil
	}

	i := 0
	var err error
	reserveLog := bytes.NewBufferString(fmt.Sprintf("New pool amounts on host %v:", hostID))
	for _, rr := range resourceRequests {
		p, ok := h.pools[rr.Resource]
		if !ok {
			logrus.Warnf("Pool %v for host %v not found for reserving %v. Skipping reservation", rr.Resource, hostID, rr)
			continue
		}

		if !force && p.used+rr.Amount > p.total {
			err = OverReserveError{hostID: hostID, resourceRequest: rr}
			break
		}

		p.used = p.used + rr.Amount
		i++

		reserveLog.WriteString(fmt.Sprintf(" %v total: %v used: %v.", rr.Resource, p.total, p.used))
	}

	if err == nil {
		logrus.Info(reserveLog.String())
	} else {
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
		logrus.Infof("Host %v not found for releasing %v. Nothing to do.", hostID, resourceRequests)
		return nil
	}

	releaseLog := bytes.NewBufferString(fmt.Sprintf("New pool amounts on host %v:", hostID))
	for _, rr := range resourceRequests {
		p, ok := h.pools[rr.Resource]
		if !ok {
			logrus.Infof("Host %v doesn't have resource pool %v. Nothing to do.", hostID, rr.Resource)
			continue
		}

		if p.used-rr.Amount < 0 {
			logrus.Infof("Decreasing used for %v.%v by %v would result in negative usage. Setting to 0.", hostID, rr.Resource, rr.Amount)
			p.used = 0
		} else {
			p.used = p.used - rr.Amount
		}

		releaseLog.WriteString(fmt.Sprintf(" %v total: %v used: %v.", rr.Resource, p.total, p.used))
	}

	logrus.Info(releaseLog.String())
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
		return fmt.Errorf("Pool %v already exists on host %v", resource, hostUUID)
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

func (s *Scheduler) reserveTempPool(hostID string, requests []ResourceRequest) {
	if s.sleepTime >= 0 {
		for _, rr := range requests {
			pool := s.hosts[hostID].pools[rr.Resource]
			pool.used += rr.Amount
			go func(amount int64, t int) {
				time.Sleep(time.Second * time.Duration(t))
				s.mu.Lock()
				pool.used -= amount
				s.mu.Unlock()
			}(rr.Amount, s.sleepTime)
		}
	}
}
