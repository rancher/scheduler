package scheduler

// Filter is an interface to prioritize and filter the hosts
type Filter interface {
	Filter(scheduler *Scheduler, resourceRequest []ResourceRequest, context Context, hosts []string) []string
}

// ReserveAction is an interface to provide a way to reserve a resource in scheduler
type ReserveAction interface {
	Reserve(scheduler *Scheduler, requests []ResourceRequest, context Context, host *host, force bool, data map[string]interface{}) error
	RollBack(scheduler *Scheduler, requests []ResourceRequest, context Context, host *host)
}

// ReleaseAction is an interface to provide a way to release a resource in scheduler
type ReleaseAction interface {
	Release(scheduler *Scheduler, requests []ResourceRequest, context Context, host *host)
}

// getFilters provides a list of filter. It provides a gateway to register a new filter.
func getFilters() []Filter {
	filters := []Filter{}

	// add ComputeFilter
	filters = append(filters, ComputeFilter{})

	// add PortFilter
	filters = append(filters, PortFilter{})

	// add LabelFilter
	filters = append(filters, LabelFilter{})

	return filters
}

// getReserveActions provides a list of reserve action. It is a gateway to register a new reserve action
func getReserveActions() []ReserveAction {
	actions := []ReserveAction{}

	// add ComputeReserveAction
	actions = append(actions, &ComputeReserveAction{})

	// add PortReserveAction
	actions = append(actions, &PortReserveAction{})

	return actions
}

// getReleaseActions provides a list of release action. It is a gateway to register a new release action
func getReleaseActions() []ReleaseAction {
	actions := []ReleaseAction{}

	// add ComputeReleaseAction
	actions = append(actions, ComputeReleaseAction{})

	// add PortReleaseAction
	actions = append(actions, PortReleaseAction{})

	return actions
}
