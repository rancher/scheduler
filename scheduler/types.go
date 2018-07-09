package scheduler

import (
	"github.com/rancher/go-rancher-metadata/metadata"
	"github.com/rancher/log"
)

type ResourceUpdater interface {
	CreateResourcePool(hostUUID string, pool ResourcePool) error
	UpdateResourcePool(hostUUID string, pool ResourcePool) bool
	RemoveHost(hostUUID string)
	CompareHostLabels(hosts []metadata.Host) bool
	UpdateWithMetadata(force bool) (bool, error)
	GetMetadataClient() metadata.Client
	SetMetadataClient(client metadata.Client)
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
	ResourceUUID string
	PortRequests []PortSpec
}

func (p PortBindingResourceRequest) GetResourceType() string {
	return p.Resource
}

type ResourcePool interface {
	GetPoolResourceType() string
	GetPoolType() string
	Create(host *host)
	Update(host *host)
}

type ComputeResourcePool struct {
	Resource  string
	Total     int64
	Used      int64
	UpdateAll bool
}

func (c *ComputeResourcePool) GetPoolResourceType() string {
	return c.Resource
}

func (c *ComputeResourcePool) GetPoolType() string {
	return computePool
}

func (c *ComputeResourcePool) Create(host *host) {
	log.Infof("Adding resource pool [%v] with total %v and used %v for host  %v", c.Resource, c.Total, c.Used, host.id)
	host.pools[c.Resource] = &ComputeResourcePool{Total: c.Total, Used: c.Used, Resource: c.Resource}
}

func (c *ComputeResourcePool) Update(host *host) {
	e := host.pools[c.Resource].(*ComputeResourcePool)
	if c.UpdateAll {
		log.Infof("Updating resource pool [%v] with total %v and used %v for host  %v", c.Resource, c.Total, c.Used, host.id)
		host.pools[c.Resource] = &ComputeResourcePool{Total: c.Total, Used: c.Used, Resource: c.Resource}
	} else {
		if e.Total != c.Total {
			log.Infof("Updating resource pool [%v] to %v for host %v", c.GetPoolResourceType(), c.Total, host.id)
			e.Total = c.Total
		}
	}
}

type PortResourcePool struct {
	Resource          string
	PortBindingMapTCP map[string]map[int64]string
	PortBindingMapUDP map[string]map[int64]string
	GhostMapTCP       map[string]map[int64]string
	GhostMapUDP       map[string]map[int64]string
	ShouldUpdate      bool
}

func (p *PortResourcePool) GetPoolResourceType() string {
	return p.Resource
}

func (p *PortResourcePool) GetPoolType() string {
	return portPoolType
}

func (p *PortResourcePool) Create(host *host) {
	ipset := []string{}
	for ip := range p.PortBindingMapTCP {
		ipset = append(ipset, ip)
	}
	log.Infof("Adding resource pool [%v], ip set %v, ports map tcp %v, ports map udp %v for host %v", p.Resource,
		ipset, p.PortBindingMapTCP, p.PortBindingMapUDP, host.id)
	host.pools[p.Resource] = p
}

func (p *PortResourcePool) Update(host *host) {
	if p.ShouldUpdate {
		ipset := []string{}
		for ip := range p.PortBindingMapTCP {
			ipset = append(ipset, ip)
		}
		log.Infof("Updating resource pool [%v], ip set %v, ports map tcp %v, ports map udp %v for host %v", p.Resource,
			ipset, p.PortBindingMapTCP, p.PortBindingMapUDP, host.id)
		host.pools[p.Resource] = p
	}
}

type LabelPool struct {
	Resource string
	Labels   map[string]string
}

func (p *LabelPool) GetPoolResourceType() string {
	return p.Resource
}

func (p *LabelPool) GetPoolType() string {
	return labelPool
}

func (p *LabelPool) Create(host *host) {
	log.Infof("Adding resource pool [%v] with label map [%v]", p.Resource, p.Labels)
	host.pools[p.Resource] = p
}

func (p *LabelPool) Update(host *host) {
	host.pools[p.Resource] = p
}

type Context []contextStruct

type contextStruct struct {
	Name      string      `json:"name"`
	ID        int         `json:"id"`
	State     string      `json:"state"`
	Domain    interface{} `json:"domain"`
	AccountID int         `json:"accountId"`
	ZoneID    int         `json:"zoneId"`
	Kind      string      `json:"kind"`
	AgentID   interface{} `json:"agentId"`
	Data      struct {
		Fields struct {
			ServiceIndex     string `json:"serviceIndex"`
			ImageUUID        string `json:"imageUuid"`
			DataVolumeMounts struct {
			} `json:"dataVolumeMounts"`
			AllocatedIPAddress           interface{} `json:"allocatedIpAddress"`
			PublishAllPorts              bool        `json:"publishAllPorts"`
			StartOnCreate                bool        `json:"startOnCreate"`
			Labels                       map[string]string
			NetworkMode                  string        `json:"networkMode"`
			DNS                          []string      `json:"dns"`
			DNSSearch                    []string      `json:"dnsSearch"`
			StdinOpen                    bool          `json:"stdinOpen"`
			Vcpu                         int           `json:"vcpu"`
			Ports                        []interface{} `json:"ports"`
			ReadOnly                     bool          `json:"readOnly"`
			DataVolumesFromLaunchConfigs []interface{} `json:"dataVolumesFromLaunchConfigs"`
			Secrets                      []interface{} `json:"secrets"`
			DataVolumesFrom              []interface{} `json:"dataVolumesFrom"`
			CapAdd                       []interface{} `json:"capAdd"`
			Tty                          bool          `json:"tty"`
			CapDrop                      []interface{} `json:"capDrop"`
			DataVolumes                  []interface{} `json:"dataVolumes"`
			Privileged                   bool          `json:"privileged"`
			LogConfig                    struct {
				Driver string `json:"driver"`
				Config struct {
				} `json:"config"`
			} `json:"logConfig"`
			Devices               []interface{} `json:"devices"`
			NetworkIds            []int         `json:"networkIds"`
			TransitioningMessage  string        `json:"transitioningMessage"`
			TransitioningProgress int           `json:"transitioningProgress"`
		} `json:"fields"`
	} `json:"data"`
	ExternalID            interface{} `json:"externalId"`
	Hostname              interface{} `json:"hostname"`
	MemoryReservation     interface{} `json:"memoryReservation"`
	RegistryCredentialID  interface{} `json:"registryCredentialId"`
	ImageID               int         `json:"imageId"`
	UUID                  string      `json:"uuid"`
	HealthState           interface{} `json:"healthState"`
	Token                 string      `json:"token"`
	Userdata              interface{} `json:"userdata"`
	CreateIndex           int         `json:"createIndex"`
	MemoryMb              interface{} `json:"memoryMb"`
	ServiceIndexID        int         `json:"serviceIndexId"`
	FirstRunning          interface{} `json:"firstRunning"`
	StartCount            int         `json:"startCount"`
	DeploymentUnitUUID    string      `json:"deploymentUnitUuid"`
	Removed               interface{} `json:"removed"`
	Created               int64       `json:"created"`
	NativeContainer       bool        `json:"nativeContainer"`
	RemoveTime            interface{} `json:"removeTime"`
	Description           interface{} `json:"description"`
	HealthUpdated         interface{} `json:"healthUpdated"`
	AllocationState       string      `json:"allocationState"`
	InstanceTriggeredStop string      `json:"instanceTriggeredStop"`
	NetworkContainerID    interface{} `json:"networkContainerId"`
	MilliCPUReservation   interface{} `json:"milliCpuReservation"`
	System                bool        `json:"system"`
	Compute               interface{} `json:"compute"`
	DNSInternal           string      `json:"dnsInternal"`
	DNSSearchInternal     string      `json:"dnsSearchInternal"`
	Version               string      `json:"version"`
}
