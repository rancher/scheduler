package scheduler

import (
	"testing"

	"gopkg.in/check.v1"
)

// gocheck setup
func Test(t *testing.T) { check.TestingT(t) }

type SchedulerTestSuite struct{}

var _ = check.Suite(&SchedulerTestSuite{})

func (s *SchedulerTestSuite) SetUpSuite(c *check.C) {
	// Nothing to setup yet
}

type rezTest struct {
	id               string
	force            bool
	resourceRequests []ResourceRequest
	expected         []string
}

func (s *SchedulerTestSuite) TestReserveResource(c *check.C) {
	scheduler := NewScheduler()

	err := scheduler.CreateResourcePool("1", &ComputeResourcePool{
		Resource: "memory",
		Total:    3,
		Used:     0,
	})
	if err != nil {
		c.Fatal(err)
	}
	err = scheduler.CreateResourcePool("1", &ComputeResourcePool{
		Resource: "storage.size",
		Total:    7,
		Used:     0,
	})
	if err != nil {
		c.Fatal(err)
	}
	err = scheduler.CreateResourcePool("2", &ComputeResourcePool{
		Resource: "memory",
		Total:    6,
		Used:     0,
	})
	if err != nil {
		c.Fatal(err)
	}
	err = scheduler.CreateResourcePool("2", &ComputeResourcePool{
		Resource: "storage.size",
		Total:    5,
		Used:     0,
	})
	if err != nil {
		c.Fatal(err)
	}

	tests := []rezTest{
		// Host 2 has more memory for first iteration
		{"1", false, []ResourceRequest{ComputeResourceRequest{Amount: 1, Resource: "memory"}, ComputeResourceRequest{Amount: 1, Resource: "storage.size"}}, []string{"2", "1"}},

		// Double memory requests result in no hosts with enough memory
		{"2-memory-requests", false, []ResourceRequest{ComputeResourceRequest{Amount: 3, Resource: "memory"}, ComputeResourceRequest{Amount: 3, Resource: "memory"}}, []string{}},

		// Host 2 still has more memory. Request has two memory resource requests
		{"2", false, []ResourceRequest{ComputeResourceRequest{Amount: 1, Resource: "memory"}, ComputeResourceRequest{Amount: 1, Resource: "memory"}, ComputeResourceRequest{Amount: 1, Resource: "storage.size"}}, []string{"2", "1"}},

		// Host 2 and 1 memory are equal and host 1 has more storage
		{"3", false, []ResourceRequest{ComputeResourceRequest{Amount: 1, Resource: "memory"}, ComputeResourceRequest{Amount: 1, Resource: "storage.size"}}, []string{"1", "2"}},

		// Host 1 again has less memory
		{"4", false, []ResourceRequest{ComputeResourceRequest{Amount: 1, Resource: "memory"}, ComputeResourceRequest{Amount: 1, Resource: "storage.size"}}, []string{"2", "1"}},

		// Memory once again equal and host 1 has more storage, use up the rest of host 1's memory
		{"5", false, []ResourceRequest{ComputeResourceRequest{Amount: 2, Resource: "memory"}, ComputeResourceRequest{Amount: 1, Resource: "storage.size"}}, []string{"1", "2"}},

		// Only host 2 is left and use up all its storage
		{"6", false, []ResourceRequest{ComputeResourceRequest{Amount: 1, Resource: "memory"}, ComputeResourceRequest{Amount: 2, Resource: "storage.size"}}, []string{"2"}},

		// No hosts left
		{"8", false, []ResourceRequest{ComputeResourceRequest{Amount: 1, Resource: "memory"}, ComputeResourceRequest{Amount: 1, Resource: "storage.size"}}, []string{}},

		// Host 2 still has memory
		{"9", false, []ResourceRequest{ComputeResourceRequest{Amount: 1, Resource: "memory"}}, []string{"2"}},

		// Host 1 still has storage
		{"10", false, []ResourceRequest{ComputeResourceRequest{Amount: 1, Resource: "storage.size"}}, []string{"1"}},
	}
	checkPrioritizationAndReserve(scheduler, tests, c)

	// Release the resources to put host 1 back to mem=1, storage.size=4, and host 2 to mem=1, storage.size=1
	scheduler.ReleaseResources("1", []ResourceRequest{ComputeResourceRequest{Amount: 1, Resource: "memory"}})
	scheduler.ReleaseResources("2", []ResourceRequest{ComputeResourceRequest{Amount: 1, Resource: "memory"}, ComputeResourceRequest{Amount: 1, Resource: "storage.size"}})

	tests = []rezTest{
		// Host 1 has same memory and more storage
		{"1", false, []ResourceRequest{ComputeResourceRequest{Amount: 1, Resource: "memory"}, ComputeResourceRequest{Amount: 1, Resource: "storage.size"}}, []string{"1", "2"}},
		// Host 1 out of memory
		{"2", false, []ResourceRequest{ComputeResourceRequest{Amount: 1, Resource: "memory"}, ComputeResourceRequest{Amount: 1, Resource: "storage.size"}}, []string{"2"}},
	}
	checkPrioritizationAndReserve(scheduler, tests, c)
}

func (s *SchedulerTestSuite) TestForceReserve(c *check.C) {
	scheduler := &Scheduler{
		hosts: map[string]*host{},
	}
	err := scheduler.CreateResourcePool("1", &ComputeResourcePool{
		Resource: "memory",
		Total:    1,
		Used:     0,
	})
	c.Check(err, check.IsNil)

	_, err = scheduler.ReserveResources("1", true, []ResourceRequest{ComputeResourceRequest{Amount: 10, Resource: "memory"}})
	c.Assert(err, check.IsNil)
	c.Assert(scheduler.hosts["1"].pools["memory"].(*ComputeResourcePool).Used, check.Equals, int64(10)) // assert that we over-committed
}

func (s *SchedulerTestSuite) TestRemoveHost(c *check.C) {
	scheduler := &Scheduler{
		hosts: map[string]*host{},
	}
	err := scheduler.CreateResourcePool("1", &ComputeResourcePool{
		Resource: "memory",
		Total:    1,
		Used:     0,
	})
	c.Check(err, check.IsNil)
	err = scheduler.CreateResourcePool("2", &ComputeResourcePool{
		Resource: "memory",
		Total:    2,
		Used:     0,
	})
	c.Check(err, check.IsNil)
	err = scheduler.CreateResourcePool("3", &ComputeResourcePool{
		Resource: "memory",
		Total:    3,
		Used:     0,
	})
	c.Check(err, check.IsNil)

	tests := []rezTest{{"1", false, []ResourceRequest{ComputeResourceRequest{Amount: 1, Resource: "memory"}}, []string{"3", "2", "1"}}}
	checkPrioritizationAndReserve(scheduler, tests, c)

	scheduler.RemoveHost("2")

	checkPrioritizationAndReserve(scheduler, []rezTest{{"1", false, []ResourceRequest{ComputeResourceRequest{Amount: 1, Resource: "memory"}}, []string{"3", "1"}}}, c)
}

func (s *SchedulerTestSuite) TestBadResourceReservation(c *check.C) {
	scheduler := &Scheduler{
		hosts: map[string]*host{},
	}
	err := scheduler.CreateResourcePool("1", &ComputeResourcePool{
		Resource: "memory",
		Total:    3,
		Used:     0,
	})
	if err != nil {
		c.Fatal(err)
	}
	err = scheduler.CreateResourcePool("1", &ComputeResourcePool{
		Resource: "storage.size",
		Total:    2,
		Used:     0,
	})
	if err != nil {
		c.Fatal(err)
	}

	// pool already on host, should just get updated
	err = scheduler.CreateResourcePool("1", &ComputeResourcePool{
		Resource: "storage.size",
		Total:    7,
		Used:     0,
	})
	c.Check(err, check.ErrorMatches, ".*already exists.*")

	// host doesn't exist
	_, err = scheduler.ReserveResources("2", false, []ResourceRequest{ComputeResourceRequest{Amount: 1, Resource: "memory"}})
	c.Assert(err, check.IsNil)
	err = scheduler.ReleaseResources("2", []ResourceRequest{ComputeResourceRequest{Amount: 1, Resource: "memory"}})
	c.Assert(err, check.IsNil)

	// pool doesn't exist on host
	_, err = scheduler.ReserveResources("1", false, []ResourceRequest{ComputeResourceRequest{Amount: 1, Resource: "network"}})
	c.Assert(err, check.IsNil)
	err = scheduler.ReleaseResources("1", []ResourceRequest{ComputeResourceRequest{Amount: 1, Resource: "network"}})
	c.Assert(err, check.IsNil)

	// Can't reserve more than is available
	_, err = scheduler.ReserveResources("1", false, []ResourceRequest{ComputeResourceRequest{Amount: 1, Resource: "storage.size"}, ComputeResourceRequest{Amount: 1, Resource: "memory"}})
	c.Assert(err, check.IsNil)
	_, err = scheduler.ReserveResources("1", false, []ResourceRequest{ComputeResourceRequest{Amount: 1, Resource: "storage.size"}, ComputeResourceRequest{Amount: 4, Resource: "memory"}})
	c.Assert(err, check.FitsTypeOf, OverReserveError{})
	c.Assert(err, check.ErrorMatches, ".*memory.*")
	// Assert that we rollback correctly
	c.Assert(scheduler.hosts["1"].pools["storage.size"].(*ComputeResourcePool).Used, check.Equals, int64(1))
	c.Assert(scheduler.hosts["1"].pools["memory"].(*ComputeResourcePool).Used, check.Equals, int64(1))

	// Release to above total, just sets pool.used to 0.
	err = scheduler.ReleaseResources("1", []ResourceRequest{ComputeResourceRequest{Amount: 1, Resource: "storage.size"}, ComputeResourceRequest{Amount: 4, Resource: "memory"}})
	c.Assert(err, check.IsNil)
}

func (s *SchedulerTestSuite) TestPortReservation(c *check.C) {
	scheduler := &Scheduler{
		hosts: map[string]*host{},
	}
	err := scheduler.CreateResourcePool("1", &PortResourcePool{
		Resource: "portReservation",
		PortBindingMap: map[string]map[int64]bool{
			"192.168.1.1": map[int64]bool{},
			"192.168.1.2": map[int64]bool{},
		},
	})
	if err != nil {
		c.Fatal(err)
	}
	err = scheduler.CreateResourcePool("2", &PortResourcePool{
		Resource: "portReservation",
		PortBindingMap: map[string]map[int64]bool{
			"192.168.1.3": map[int64]bool{},
			"192.168.1.4": map[int64]bool{},
		},
	})
	if err != nil {
		c.Fatal(err)
	}

	updated := scheduler.UpdateResourcePool("2", &PortResourcePool{
		Resource: "portReservation",
		PortBindingMap: map[string]map[int64]bool{
			"192.168.1.3": map[int64]bool{},
			"192.168.1.4": map[int64]bool{},
			"192.168.1.5": map[int64]bool{},
		},
	})
	c.Assert(updated, check.Equals, true)

	// testing port pool has been updated
	c.Assert(len(scheduler.hosts["2"].pools["portReservation"].(*PortResourcePool).PortBindingMap), check.Equals, 3)
	specs := []PortSpec{{PublicPort: 8081, PrivatePort: 8081}, {PublicPort: 8082, PrivatePort: 8082}}
	data, err := scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: "1", Resource: "portReservation", PortRequests: specs}})
	c.Assert(err, check.IsNil)
	c.Assert(data["portReservation"].(map[string]interface{})["allocatedIPs"], check.HasLen, 2)
	data, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: "1", Resource: "portReservation", PortRequests: specs}})
	c.Assert(err, check.IsNil)
	c.Assert(data["portReservation"].(map[string]interface{})["allocatedIPs"], check.HasLen, 2)
	// the next action should error out, since there is no port available

	data, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: "1", Resource: "portReservation", PortRequests: specs}})
	c.Assert(err, check.NotNil)

	releaseSpec := []PortSpec{{IPAddress: "192.168.1.1", PublicPort: 8081, PrivatePort: 8081}, {IPAddress: "192.168.1.2", PublicPort: 8082, PrivatePort: 8082}}
	err = scheduler.ReleaseResources("1", []ResourceRequest{PortBindingResourceRequest{InstanceID: "1", Resource: "portReservation", PortRequests: releaseSpec}})
	c.Assert(err, check.IsNil)

	data, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: "1", Resource: "portReservation", PortRequests: specs}})
	c.Assert(err, check.IsNil)
	c.Assert(data["portReservation"].(map[string]interface{})["allocatedIPs"], check.HasLen, 2)

	// test roll back logic
	specs = []PortSpec{{PublicPort: 8083, PrivatePort: 8083}, {PublicPort: 8081, PrivatePort: 8081}}
	data, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: "1", Resource: "portReservation", PortRequests: specs}})
	c.Assert(err, check.NotNil)
	// assert that we still have 2 slots for 8083, bc it just rolled back
	c.Assert(getPortSlots(scheduler.hosts["1"].pools["portReservation"].(*PortResourcePool), 8083), check.Equals, 2)

	// test random port allocation
	specs = []PortSpec{{PrivatePort: 8083}}
	data, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: "1", Resource: "portReservation", PortRequests: specs}})
	port := data["portReservation"].(map[string]interface{})["allocatedIPs"].([]map[string]interface{})[0]["publicPort"].(int64)
	if port < 32768 || port > 61000 {
		c.Fatalf("Random Port Allocation failed. Port allocated %v", port)
	}

	// test 192.168.1.1:8084:8084, should only reserve port from 192.168.1.1
	specs = []PortSpec{{IPAddress: "192.168.1.1", PublicPort: 8084, PrivatePort: 8084}}
	_, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: "1", Resource: "portReservation", PortRequests: specs}})
	c.Assert(getPortSlots(scheduler.hosts["1"].pools["portReservation"].(*PortResourcePool), 8084), check.Equals, 1)

	// test 0.0.0.0:8084:8084, should reserve port on all ip4 address
	specs = []PortSpec{{IPAddress: "0.0.0.0", PublicPort: 8085, PrivatePort: 8085}}
	_, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: "1", Resource: "portReservation", PortRequests: specs}})
	c.Assert(getPortSlots(scheduler.hosts["1"].pools["portReservation"].(*PortResourcePool), 8085), check.Equals, 0)
}

func getPortSlots(pool *PortResourcePool, port int64) int {
	slots := 0
	for _, portMap := range pool.PortBindingMap {
		if !portMap[port] {
			slots++
		}
	}
	return slots
}

func checkPrioritizationAndReserve(scheduler *Scheduler, tests []rezTest, c *check.C) {
	for _, t := range tests {
		actual, err := scheduler.PrioritizeCandidates(t.resourceRequests)
		if err != nil {
			c.Fatal(err)
		}
		c.Logf("Checking %v", t.id)
		c.Assert(actual, check.DeepEquals, t.expected)

		if len(t.expected) > 0 {
			_, err = scheduler.ReserveResources(t.expected[0], t.force, t.resourceRequests)
			c.Assert(err, check.IsNil)
		}
	}
}
