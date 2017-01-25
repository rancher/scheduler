package scheduler

type ResourceUpdater interface {
	CreateResourcePool(hostUUID string, pool ResourcePool) error
	UpdateResourcePool(hostUUID string, pool ResourcePool) bool
	RemoveHost(hostUUID string)
}

type ResourceRequest interface {
	GetResourceType() string
}

type BaseResourceRequest struct {
	Resource string
	Type     string
}

func (b BaseResourceRequest) GetResourceType() string {
	return b.Resource
}

type AmountBasedResourceRequest struct {
	Resource string
	Amount   int64
}

func (c AmountBasedResourceRequest) GetResourceType() string {
	return c.Resource
}

type PortSpec struct {
	PrivatePort int64
	IPAddress   string
	PublicPort  int64
	Protocol    string
}

type PortBindingResourceRequest struct {
	Resource     string
	InstanceID   string
	PortRequests []PortSpec
}

func (p PortBindingResourceRequest) GetResourceType() string {
	return p.Resource
}

type ResourcePool interface {
	GetPoolResourceType() string
	GetPoolType() string
}

type ComputeResourcePool struct {
	Resource string
	Total    int64
	Used     int64
}

func (c *ComputeResourcePool) GetPoolResourceType() string {
	return c.Resource
}

func (c *ComputeResourcePool) GetPoolType() string {
	return computePool
}

type PortResourcePool struct {
	Resource          string
	PortBindingMapTCP map[string]map[int64]bool
	PortBindingMapUDP map[string]map[int64]bool
	GhostMapTCP       map[string]map[int64]bool
	GhostMapUDP       map[string]map[int64]bool
}

func (p *PortResourcePool) GetPoolResourceType() string {
	return p.Resource
}

func (p *PortResourcePool) GetPoolType() string {
	return portPool
}
