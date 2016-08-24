package scheduler

func filter(hosts map[string]*host, resourceRequests []ResourceRequest) []*host {
	filtered := []*host{}
Outer:
	for _, h := range hosts {
		for _, rr := range resourceRequests {
			pool, ok := h.pools[rr.Resource]
			if !ok || (pool.total-pool.used) < rr.Amount {
				continue Outer
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
		iPool, iOK := s.hosts[i].pools[rr.Resource]
		jPool, jOK := s.hosts[j].pools[rr.Resource]

		if iOK && !jOK {
			return true
		} else if !iOK {
			return false
		}

		iAvailable := iPool.total - iPool.used
		jAvailable := jPool.total - jPool.used

		if iAvailable > jAvailable {
			return true
		} else if iAvailable < jAvailable {
			return false
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
