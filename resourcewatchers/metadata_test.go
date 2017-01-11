package resourcewatchers

import (
	"fmt"
	"testing"

	"github.com/rancher/go-rancher-metadata/metadata"
	"github.com/rancher/scheduler/scheduler"
	"gopkg.in/check.v1"
)

// gocheck setup
func Test(t *testing.T) { check.TestingT(t) }

type MetadataTestSuite struct{}

var _ = check.Suite(&MetadataTestSuite{})

func (s *MetadataTestSuite) SetUpSuite(c *check.C) {
	// Nothing to setup yet
}

func (s *MetadataTestSuite) TestWatchMetadata(c *check.C) {
	sched := scheduler.NewScheduler()

	change := make(chan string)
	changeDone := make(chan int)
	mock := &mockMDClient{
		change:     change,
		changeDone: changeDone,
		hosts:      defaultHosts,
	}

	go WatchMetadata(mock, sched)

	// The mock metadata client's OnChange hasn't fired yet, so there should be no valid candidates
	actual, err := sched.PrioritizeCandidates([]scheduler.ResourceRequest{{Amount: 1, Resource: "memoryReservation"}})
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{})

	// This will cause the OnChange to fire in our mock metadata client. The initial run will report that
	// host-a has 1 total and 1 used memory and host-b has 2 total and none used
	change <- "1"
	<-changeDone
	actual, err = sched.PrioritizeCandidates([]scheduler.ResourceRequest{{Amount: 1, Resource: "memoryReservation"}})
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{"host-b"})

	err = sched.ReserveResources("host-b", false, []scheduler.ResourceRequest{{Amount: 2, Resource: "memoryReservation"}})
	c.Assert(err, check.IsNil)

	// Release the initially used memory from host-a
	err = sched.ReleaseResources("host-a", []scheduler.ResourceRequest{{Amount: 1, Resource: "memoryReservation"}})

	actual, err = sched.PrioritizeCandidates([]scheduler.ResourceRequest{{Amount: 1, Resource: "memoryReservation"}})
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{"host-a"})

	// Test "removing" host-a
	mock.hosts = []metadata.Host{{UUID: "host-b", Memory: 1}}
	change <- "2"
	<-changeDone

	actual, err = sched.PrioritizeCandidates([]scheduler.ResourceRequest{{Amount: 1, Resource: "memoryReservation"}})
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{})
}

func (s *MetadataTestSuite) TestPanicLogic(c *check.C) {
	sched := scheduler.NewScheduler()

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

var defaultHosts = []metadata.Host{{UUID: "host-a", Memory: 1}, {UUID: "host-b", Memory: 2}}

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
