package scheduler

func filter(hosts map[string]*host, resourceRequests []ResourceRequest) []*host {
	filtered := []*host{}
	aggregateResReqs := map[string]ResourceRequest{}
	for _, rr := range resourceRequests {
		if request, ok := rr.(ComputeResourceRequest); ok {
			aggResReq, ok := aggregateResReqs[request.Resource].(ComputeResourceRequest)
			if !ok {
				aggResReq = ComputeResourceRequest{
					Resource: rr.GetResourceType(),
					Amount:   0,
				}
				aggregateResReqs[rr.GetResourceType()] = aggResReq
			}
			aggResReq.Amount += request.Amount
			aggregateResReqs[rr.GetResourceType()] = aggResReq
		}
	}

Outer:
	for _, h := range hosts {
		for _, rr := range aggregateResReqs {
			if rq, ok := rr.(ComputeResourceRequest); ok {
				pool, ok := h.pools[rr.GetResourceType()].(*ComputeResourcePool)
				if !ok || (pool.Total-pool.Used) < rq.Amount {
					continue Outer
				}
			}
		}
		filtered = append(filtered, h)
	}

	return filtered
}

type hostSorter struct {
	hosts            []*host
	resourceRequests []ResourceRequest
}

func (s hostSorter) Len() int {
	return len(s.hosts)
}

func (s hostSorter) Swap(i, j int) {
	s.hosts[i], s.hosts[j] = s.hosts[j], s.hosts[i]
}

func (s hostSorter) Less(i, j int) bool {
	for _, rr := range s.resourceRequests {
		if rq, ok := rr.(ComputeResourceRequest); ok {
			iPool, iOK := s.hosts[i].pools[rq.Resource].(*ComputeResourcePool)
			jPool, jOK := s.hosts[j].pools[rq.Resource].(*ComputeResourcePool)

			if iOK && !jOK {
				return true
			} else if !iOK {
				return false
			}

			iAvailable := iPool.Total - iPool.Used
			jAvailable := jPool.Total - jPool.Used

			if iAvailable > jAvailable {
				return true
			} else if iAvailable < jAvailable {
				return false
			}
		}
	}
	return false
}

func ids(hosts []*host) []string {
	ids := []string{}
	for _, h := range hosts {
		ids = append(ids, h.id)
	}

	return ids
}
