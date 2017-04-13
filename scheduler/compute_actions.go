package scheduler

import (
	"bytes"
	"fmt"
	"github.com/Sirupsen/logrus"
)

// ComputeFilter define a filter based on cpu, memory and instance number
type ComputeFilter struct {
}

func (c ComputeFilter) Filter(scheduler *Scheduler, resourceRequests []ResourceRequest, context Context, hosts []string) []string {
	filteredHosts := filter(scheduler.hosts, resourceRequests)
	result := []string{}
	for _, host := range filteredHosts {
		result = append(result, host.id)
	}
	return result
}

// ComputeReserveAction is a reserve action for cpu, memory, instance count and other compute resource.
type ComputeReserveAction struct {
	offset int
}

func (c *ComputeReserveAction) Reserve(scheduler *Scheduler, requests []ResourceRequest, context Context, host *host, force bool, data map[string]interface{}) error {
	var err error
	var reserveLog *bytes.Buffer
	for _, rr := range requests {
		p, ok := host.pools[rr.GetResourceType()]
		if !ok {
			logrus.Warnf("Pool %v for host %v not found for reserving %v. Skipping reservation", rr.GetResourceType(), host.id, rr)
			continue
		}
		PoolType := p.GetPoolType()
		if PoolType == computePool {
			pool := p.(*ComputeResourcePool)
			request := rr.(AmountBasedResourceRequest)
			if !force && pool.Used+request.Amount > pool.Total {
				err = OverReserveError{hostID: host.id, resourceRequest: rr}
				return err
			}

			pool.Used = pool.Used + request.Amount
			c.offset = c.offset + 1
			if reserveLog == nil {
				reserveLog = bytes.NewBufferString(fmt.Sprintf("New pool amount on host %v:", host.id))
			}
			reserveLog.WriteString(fmt.Sprintf(" %v total: %v used: %v.", request.Resource, pool.Total, pool.Used))
		}
	}
	if reserveLog != nil {
		logrus.Info(reserveLog.String())
	}
	return nil
}

func (c *ComputeReserveAction) RollBack(scheduler *Scheduler, requests []ResourceRequest, context Context, host *host) {
	for _, rr := range requests[:c.offset] {
		p, ok := host.pools[rr.GetResourceType()]
		if !ok {
			break
		}
		resourcePoolType := p.GetPoolType()
		if resourcePoolType == computePool {
			pool := p.(*ComputeResourcePool)
			request := rr.(AmountBasedResourceRequest)
			pool.Used = pool.Used - request.Amount
		}
	}
	c.offset = 0
}

// ComputeReleaseAction is a release action to release cpu, memory, instance counts and other compute resources
type ComputeReleaseAction struct{}

func (c ComputeReleaseAction) Release(scheduler *Scheduler, requests []ResourceRequest, context Context, host *host) {
	releaseLog := bytes.NewBufferString(fmt.Sprintf("New pool amounts on host %v:", host.id))
	for _, rr := range requests {
		p, ok := host.pools[rr.GetResourceType()]
		if !ok {
			logrus.Infof("Host %v doesn't have resource pool %v. Nothing to do.", host.id, rr.GetResourceType())
			continue
		}
		PoolType := p.GetPoolType()
		if PoolType == computePool {
			pool := p.(*ComputeResourcePool)
			request := rr.(AmountBasedResourceRequest)
			if pool.Used-request.Amount < 0 {
				logrus.Infof("Decreasing used for %v.%v by %v would result in negative usage. Setting to 0.", host.id, request.Resource, request.Amount)
				pool.Used = 0
			} else {
				pool.Used = pool.Used - request.Amount
			}
			releaseLog.WriteString(fmt.Sprintf(" %v total: %v used: %v.", request.Resource, pool.Total, pool.Used))
		}
	}
	logrus.Info(releaseLog.String())
}

type OverReserveError struct {
	hostID          string
	resourceRequest ResourceRequest
}

func (e OverReserveError) Error() string {
	return fmt.Sprintf("Not enough available resources on host %v to reserve %v.", e.hostID, e.resourceRequest)
}
