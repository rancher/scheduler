package resourcewatchers

import (
	"fmt"
	"testing"

	check "gopkg.in/check.v1"

	"github.com/rancher/go-rancher-metadata/metadata"
	"github.com/rancher/go-rancher/v2"
	"github.com/rancher/scheduler/scheduler"
)

// gocheck setup
func Test(t *testing.T) { check.TestingT(t) }

type MetadataTestSuite struct{}

var _ = check.Suite(&MetadataTestSuite{})

func (s *MetadataTestSuite) SetUpSuite(c *check.C) {
	// Nothing to setup yet
}

func (s *MetadataTestSuite) TestWatchMetadata(c *check.C) {
	sched := scheduler.NewScheduler(-1)

	change := make(chan string)
	changeDone := make(chan int)
	mock := &mockMDClient{
		change:     change,
		changeDone: changeDone,
		hosts:      defaultHosts,
	}

	go WatchMetadata(mock, sched, nil)

	// The mock metadata client's OnChange hasn't fired yet, so there should be no valid candidates
	actual, err := sched.PrioritizeCandidates([]scheduler.ResourceRequest{scheduler.AmountBasedResourceRequest{Amount: 1, Resource: "memoryReservation"}}, scheduler.Context{})
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{})

	// This will cause the OnChange to fire in our mock metadata client. The initial run will report that
	// host-a has 1 total and 1 used memory and host-b has 2 total and none used
	change <- "1"
	<-changeDone
	actual, err = sched.PrioritizeCandidates([]scheduler.ResourceRequest{scheduler.AmountBasedResourceRequest{Amount: 1, Resource: "memoryReservation"}}, scheduler.Context{})
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{"host-b"})

	_, err = sched.ReserveResources("host-b", false, []scheduler.ResourceRequest{scheduler.AmountBasedResourceRequest{Amount: 2, Resource: "memoryReservation"}}, nil)
	c.Assert(err, check.IsNil)

	// Release the initially used memory from host-a
	err = sched.ReleaseResources("host-a", []scheduler.ResourceRequest{scheduler.AmountBasedResourceRequest{Amount: 1, Resource: "memoryReservation"}}, nil)

	actual, err = sched.PrioritizeCandidates([]scheduler.ResourceRequest{scheduler.AmountBasedResourceRequest{Amount: 1, Resource: "memoryReservation"}}, scheduler.Context{})
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{"host-a"})

	// Test "removing" host-a
	mock.hosts = []metadata.Host{{UUID: "host-b", Memory: 1}}
	change <- "2"
	<-changeDone

	actual, err = sched.PrioritizeCandidates([]scheduler.ResourceRequest{scheduler.AmountBasedResourceRequest{Amount: 1, Resource: "memoryReservation"}}, scheduler.Context{})
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{})
}

func (s *MetadataTestSuite) TestWatchMetadataPortPool(c *check.C) {
	sched := scheduler.NewScheduler(-1)

	change := make(chan string)
	changeDone := make(chan int)
	mock := &mockMDPortClient{
		change:     change,
		changeDone: changeDone,
		hosts:      portUsedHosts,
	}

	go WatchMetadata(mock, sched, nil)

	// The mock metadata client's OnChange hasn't fired yet, so there should be no valid candidates
	actual, err := sched.PrioritizeCandidates([]scheduler.ResourceRequest{scheduler.PortBindingResourceRequest{InstanceID: "1", ResourceUUID: "12345", Resource: "portReservation", PortRequests: []scheduler.PortSpec{{PublicPort: 8081, PrivatePort: 8081, Protocol: "tcp"}}}}, scheduler.Context{})
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{})

	change <- "1"
	<-changeDone
	// port 8081 is already used by host-a, so return host-b
	actual, err = sched.PrioritizeCandidates([]scheduler.ResourceRequest{scheduler.PortBindingResourceRequest{InstanceID: "1", ResourceUUID: "12345", Resource: "portReservation", PortRequests: []scheduler.PortSpec{{PublicPort: 8081, PrivatePort: 8081, Protocol: "tcp"}}}}, scheduler.Context{})
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{"host-b"})

	// port 8082 is used for udp by host-a, so return host-b
	actual, err = sched.PrioritizeCandidates([]scheduler.ResourceRequest{scheduler.PortBindingResourceRequest{InstanceID: "1", ResourceUUID: "12345", Resource: "portReservation", PortRequests: []scheduler.PortSpec{{PublicPort: 8082, PrivatePort: 8082, Protocol: "udp"}}}}, scheduler.Context{})
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{"host-b"})

	// port 8083 is not used, should return two host
	actual, err = sched.PrioritizeCandidates([]scheduler.ResourceRequest{scheduler.PortBindingResourceRequest{InstanceID: "1", ResourceUUID: "12345", Resource: "portReservation", PortRequests: []scheduler.PortSpec{{PublicPort: 8083, PrivatePort: 8083, Protocol: "tcp"}}}}, scheduler.Context{})
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.HasLen, 2)

	// release the port 8081
	err = sched.ReleaseResources("host-a", []scheduler.ResourceRequest{scheduler.PortBindingResourceRequest{InstanceID: "1", ResourceUUID: "12345", Resource: "portReservation", PortRequests: []scheduler.PortSpec{{IPAddress: "192.168.1.1", PublicPort: 8081, PrivatePort: 8081, Protocol: "tcp"}}}}, nil)

	// when 8081 is released, scheduler should return two host available
	actual, err = sched.PrioritizeCandidates([]scheduler.ResourceRequest{scheduler.PortBindingResourceRequest{InstanceID: "1", ResourceUUID: "12345", Resource: "portReservation", PortRequests: []scheduler.PortSpec{{PublicPort: 8081, PrivatePort: 8081, Protocol: "tcp"}}}}, scheduler.Context{})
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.HasLen, 2)
}

func (s *MetadataTestSuite) TestIPLabelChange(c *check.C) {
	sched := scheduler.NewScheduler(-1)

	change := make(chan string)
	changeDone := make(chan int)

	h1 := []metadata.Host{{UUID: "host-a", Memory: 1}}

	h2 := []metadata.Host{{UUID: "host-a", Memory: 1, Labels: map[string]string{"io.rancher.scheduler.ips": "192.168.1.1,192.168.1.2"}}}

	mock := &mockMDClient{
		change:     change,
		changeDone: changeDone,
		hosts:      h1,
	}

	go WatchMetadata(mock, sched, nil)
	// this should populate 0.0.0.0 for both host
	change <- "1"
	<-changeDone

	mock.hosts = h2
	// this should populate ips labels to h2
	change <- "2"
	<-changeDone

	r1 := []scheduler.ResourceRequest{scheduler.PortBindingResourceRequest{InstanceID: "1", ResourceUUID: "12345", Resource: "portReservation", PortRequests: []scheduler.PortSpec{{PublicPort: 8081, PrivatePort: 8081, Protocol: "tcp"}}}}
	r2 := []scheduler.ResourceRequest{scheduler.PortBindingResourceRequest{InstanceID: "2", ResourceUUID: "12346", Resource: "portReservation", PortRequests: []scheduler.PortSpec{{PublicPort: 8081, PrivatePort: 8081, Protocol: "tcp"}}}}
	_, err := sched.ReserveResources("host-a", false, r1, nil)
	c.Assert(err, check.IsNil)
	_, err = sched.ReserveResources("host-a", false, r2, nil)
	c.Assert(err, check.IsNil)
}

func (s *MetadataTestSuite) TestPanicLogic(c *check.C) {
	sched := scheduler.NewScheduler(-1)

	change := make(chan string)
	changeDone := make(chan int)
	mock := &mockMDClient{
		change:     change,
		changeDone: changeDone,
		errorHosts: true,
	}

	w := &metadataWatcher{
		resourceUpdater: sched,
		client:          mock,
	}

	w.updateFromMetadata("1")
	w.updateFromMetadata("2")
	w.updateFromMetadata("3")
	w.updateFromMetadata("4")
	w.updateFromMetadata("5")
	c.Assert(func() { w.updateFromMetadata("6") }, check.PanicMatches, ".*6 consecutive errors.*")
}

func (s *MetadataTestSuite) TestExternalHostEvents(c *check.C) {
	sched := scheduler.NewScheduler(-1)

	change := make(chan string)
	changeDone := make(chan int)
	mock := &mockMDClient{
		change:     change,
		changeDone: changeDone,
		hosts:      hostsWithLabels,
	}
	mockPublishOps := &mockHostEvent{}
	mockClient := &client.RancherClient{
		ExternalHostEvent: mockPublishOps,
	}
	go WatchMetadata(mock, sched, mockClient)

	// fire the first time, should publish
	change <- "1"
	<-changeDone
	c.Assert(mockPublishOps.ExternalHostEvent.EventType, check.Equals, "scheduler.update")
	mockPublishOps.ExternalHostEvent = nil

	//fire the second time, since label hasn't changed, no update
	change <- "2"
	<-changeDone
	c.Assert(mockPublishOps.ExternalHostEvent, check.IsNil)

	// change the label, should publish
	mock.hosts = []metadata.Host{{UUID: "host-a", Labels: map[string]string{"foo": "bar1"}}}
	change <- "3"
	<-changeDone
	c.Assert(mockPublishOps.ExternalHostEvent.EventType, check.Equals, "scheduler.update")
	mockPublishOps.ExternalHostEvent = nil

	//change other things, no update
	mock.hosts = []metadata.Host{{UUID: "host-a", Labels: map[string]string{"foo": "bar1"}, Memory: 10}}
	change <- "4"
	<-changeDone
	c.Assert(mockPublishOps.ExternalHostEvent, check.IsNil)

	//add new hosts, should fire
	mock.hosts = []metadata.Host{{UUID: "host-a", Labels: map[string]string{"foo": "bar1"}, Memory: 10}, {UUID: "host-b", Labels: map[string]string{"foo": "bar2"}}}
	change <- "5"
	<-changeDone
	c.Assert(mockPublishOps.ExternalHostEvent.EventType, check.Equals, "scheduler.update")
}

var defaultHosts = []metadata.Host{{UUID: "host-a", Memory: 1}, {UUID: "host-b", Memory: 2}}

var hostsWithLabels = []metadata.Host{{UUID: "host-a", Labels: map[string]string{"foo": "bar"}}}

var portUsedHosts = []metadata.Host{{UUID: "host-a", Labels: map[string]string{"io.rancher.scheduler.ips": "192.168.1.1,192.168.1.2"}}, {UUID: "host-b", Labels: map[string]string{"io.rancher.scheduler.ips": "192.168.1.3,192.168.1.4"}}}

type mockMDClient struct {
	metadata.Client
	change          chan string
	changeDone      chan int
	errorHosts      bool
	errorContainers bool
	hosts           []metadata.Host
}

func (c *mockMDClient) OnChangeWithError(intervalSeconds int, do func(string)) error {
	for change := range c.change {
		do(change)
		c.changeDone <- 1
	}
	return nil
}

func (c *mockMDClient) GetHosts() ([]metadata.Host, error) {
	if c.errorHosts {
		return nil, fmt.Errorf("Doesn't work")
	}
	return c.hosts, nil
}

func (c *mockMDClient) GetContainers() ([]metadata.Container, error) {
	if c.errorContainers {
		return nil, fmt.Errorf("Doesn't work")
	}
	return []metadata.Container{{MemoryReservation: 1, HostUUID: "host-a"}}, nil
}

func (c *mockMDClient) GetServices() ([]metadata.Service, error) {
	return []metadata.Service{}, nil
}

type mockMDPortClient struct {
	metadata.Client
	change          chan string
	changeDone      chan int
	errorHosts      bool
	errorContainers bool
	hosts           []metadata.Host
}

func (c *mockMDPortClient) OnChangeWithError(intervalSeconds int, do func(string)) error {
	for change := range c.change {
		do(change)
		c.changeDone <- 1
	}
	return nil
}

func (c *mockMDPortClient) GetHosts() ([]metadata.Host, error) {
	if c.errorHosts {
		return nil, fmt.Errorf("Doesn't work")
	}
	return c.hosts, nil
}

func (c *mockMDPortClient) GetContainers() ([]metadata.Container, error) {
	if c.errorContainers {
		return nil, fmt.Errorf("Doesn't work")
	}
	return []metadata.Container{
		{UUID: "12345", State: "running", Ports: []string{"192.168.1.1:8081:8081/tcp", "192.168.1.2:8081:8081/tcp", "192.168.1.1:8082:8082/udp", "192.168.1.2:8082:8082/udp"}, HostUUID: "host-a"},
		{UUID: "12346", State: "running", Ports: []string{"192.168.1.3:8081:8081/tcp", "192.168.1.4:8082:8082/tcp"}, HostUUID: "host-b"},
		{UUID: "12347", State: "stopped", Ports: []string{"192.168.1.3:8083:8083/tcp", "192.168.1.4:8082:8082/tcp"}, HostUUID: "host-b"},
	}, nil
}

func (c *mockMDPortClient) GetServices() ([]metadata.Service, error) {
	return []metadata.Service{}, nil
}

type mockHostEvent struct {
	client.ExternalHostEventOperations
	ExternalHostEvent *client.ExternalHostEvent
}

func (m *mockHostEvent) Create(event *client.ExternalHostEvent) (*client.ExternalHostEvent, error) {
	m.ExternalHostEvent = event
	return event, nil
}
