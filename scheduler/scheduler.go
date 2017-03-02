package scheduler

import (
	"fmt"
	"sync"

	"time"

	"github.com/Sirupsen/logrus"
	"github.com/rancher/go-rancher-metadata/metadata"
	"reflect"
)

const (
	hostLabels          = "hostLabels"
	computePool         = "computePool"
	portPool            = "portPool"
	instanceReservation = "instanceReservation"
	labelPool           = "labelPool"
	defaultIP           = "0.0.0.0"
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
	filteredHosts := []string{}
	for host := range s.hosts {
		filteredHosts = append(filteredHosts, host)
	}

	filters := getFilters()
	for _, filter := range filters {
		filteredHosts = filter.Filter(s, resourceRequests, context, filteredHosts)
	}
	filteredHosts = sortHosts(s, resourceRequests, context, filteredHosts)
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

	reserveActions := getReserveActions()
	data := map[string]interface{}{}

	executedActions := []ReserveAction{}

	for _, action := range reserveActions {
		err := action.Reserve(s, resourceRequests, nil, h, force, data)
		executedActions = append(executedActions, action)
		if err != nil {
			logrus.Error("Error happens in reserving resource. Rolling back the reservation")
			// rollback previous reserve actions
			for _, exeAction := range executedActions {
				exeAction.RollBack(s, resourceRequests, nil, h)
			}
			return nil, err
		}
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
	releaseActions := getReleaseActions()

	for _, rAction := range releaseActions {
		rAction.Release(s, resourceRequests, nil, h)
	}
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

	pool.Create(h)

	return nil
}

func (s *Scheduler) UpdateResourcePool(hostUUID string, pool ResourcePool) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	h, ok := s.hosts[hostUUID]
	if !ok {
		return false
	}

	_, ok = h.pools[pool.GetPoolResourceType()]
	if !ok {
		return false
	}

	pool.Update(h)

	return true
}

func (s *Scheduler) RemoveHost(hostUUID string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	logrus.Infof("Removing host %v.", hostUUID)
	delete(s.hosts, hostUUID)
}

func (s *Scheduler) CompareHostLabels(hosts []metadata.Host) bool {
	if len(s.hosts) != len(hosts) {
		return true
	}
	for _, host := range hosts {
		originalHost, ok := s.hosts[host.UUID]
		if !ok {
			return true
		}
		prevMap := originalHost.pools[hostLabels].(*LabelPool)
		currMap := host.Labels
		if !reflect.DeepEqual(prevMap.Labels, currMap) {
			return true
		}
	}
	return false
}

func (s *Scheduler) reserveTempPool(hostID string, requests []ResourceRequest) {
	if s.sleepTime >= 0 {
		for _, rr := range requests {
			if computeReq, ok := rr.(AmountBasedResourceRequest); ok {
				pool := s.hosts[hostID].pools[computeReq.Resource].(*ComputeResourcePool)
				if pool.Resource == instanceReservation {
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
}
