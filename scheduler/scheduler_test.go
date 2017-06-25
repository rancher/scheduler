package scheduler

import (
	"testing"

	check "gopkg.in/check.v1"
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
	scheduler := NewScheduler(-1)

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
		{"1", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "memory"}, AmountBasedResourceRequest{Amount: 1, Resource: "storage.size"}}, []string{"2", "1"}},

		// Double memory requests result in no hosts with enough memory
		{"2-memory-requests", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 3, Resource: "memory"}, AmountBasedResourceRequest{Amount: 3, Resource: "memory"}}, []string{}},

		// Host 2 still has more memory. Request has two memory resource requests
		{"2", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "memory"}, AmountBasedResourceRequest{Amount: 1, Resource: "memory"}, AmountBasedResourceRequest{Amount: 1, Resource: "storage.size"}}, []string{"2", "1"}},

		// Host 2 and 1 memory are equal and host 1 has more storage
		{"3", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "memory"}, AmountBasedResourceRequest{Amount: 1, Resource: "storage.size"}}, []string{"1", "2"}},

		// Host 1 again has less memory
		{"4", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "memory"}, AmountBasedResourceRequest{Amount: 1, Resource: "storage.size"}}, []string{"2", "1"}},

		// Memory once again equal and host 1 has more storage, use up the rest of host 1's memory
		{"5", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 2, Resource: "memory"}, AmountBasedResourceRequest{Amount: 1, Resource: "storage.size"}}, []string{"1", "2"}},

		// Only host 2 is left and use up all its storage
		{"6", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "memory"}, AmountBasedResourceRequest{Amount: 2, Resource: "storage.size"}}, []string{"2"}},

		// No hosts left
		{"8", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "memory"}, AmountBasedResourceRequest{Amount: 1, Resource: "storage.size"}}, []string{}},

		// Host 2 still has memory
		{"9", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "memory"}}, []string{"2"}},

		// Host 1 still has storage
		{"10", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "storage.size"}}, []string{"1"}},
	}
	checkPrioritizationAndReserve(scheduler, tests, c)

	// Release the resources to put host 1 back to mem=1, storage.size=4, and host 2 to mem=1, storage.size=1
	scheduler.ReleaseResources("1", []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "memory"}}, nil)
	scheduler.ReleaseResources("2", []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "memory"}, AmountBasedResourceRequest{Amount: 1, Resource: "storage.size"}}, nil)

	tests = []rezTest{
		// Host 1 has same memory and more storage
		{"1", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "memory"}, AmountBasedResourceRequest{Amount: 1, Resource: "storage.size"}}, []string{"1", "2"}},
		// Host 1 out of memory
		{"2", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "memory"}, AmountBasedResourceRequest{Amount: 1, Resource: "storage.size"}}, []string{"2"}},
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

	_, err = scheduler.ReserveResources("1", true, []ResourceRequest{AmountBasedResourceRequest{Amount: 10, Resource: "memory"}}, nil)
	c.Assert(err, check.IsNil)
	c.Assert(scheduler.hosts["1"].pools["memory"].(*ComputeResourcePool).Used, check.Equals, int64(10)) // assert that we over-committed
}

func (s *SchedulerTestSuite) TestRemoveHost(c *check.C) {
	scheduler := &Scheduler{
		hosts:     map[string]*host{},
		sleepTime: -1,
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

	tests := []rezTest{{"1", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "memory"}}, []string{"3", "2", "1"}}}
	checkPrioritizationAndReserve(scheduler, tests, c)

	scheduler.RemoveHost("2")

	checkPrioritizationAndReserve(scheduler, []rezTest{{"1", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "memory"}}, []string{"3", "1"}}}, c)
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
	_, err = scheduler.ReserveResources("2", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "memory"}}, nil)
	c.Assert(err, check.IsNil)
	err = scheduler.ReleaseResources("2", []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "memory"}}, nil)
	c.Assert(err, check.IsNil)

	// pool doesn't exist on host
	_, err = scheduler.ReserveResources("1", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "network"}}, nil)
	c.Assert(err, check.IsNil)
	err = scheduler.ReleaseResources("1", []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "network"}}, nil)
	c.Assert(err, check.IsNil)

	// Can't reserve more than is available
	_, err = scheduler.ReserveResources("1", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "storage.size"}, AmountBasedResourceRequest{Amount: 1, Resource: "memory"}}, nil)
	c.Assert(err, check.IsNil)
	_, err = scheduler.ReserveResources("1", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "storage.size"}, AmountBasedResourceRequest{Amount: 4, Resource: "memory"}}, nil)
	c.Assert(err, check.FitsTypeOf, OverReserveError{})
	c.Assert(err, check.ErrorMatches, ".*memory.*")
	// Assert that we rollback correctly
	c.Assert(scheduler.hosts["1"].pools["storage.size"].(*ComputeResourcePool).Used, check.Equals, int64(1))
	c.Assert(scheduler.hosts["1"].pools["memory"].(*ComputeResourcePool).Used, check.Equals, int64(1))

	// Release to above total, just sets pool.used to 0.
	err = scheduler.ReleaseResources("1", []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "storage.size"}, AmountBasedResourceRequest{Amount: 4, Resource: "memory"}}, nil)
	c.Assert(err, check.IsNil)
}

func (s *SchedulerTestSuite) TestPortReservation(c *check.C) {
	scheduler := &Scheduler{
		hosts: map[string]*host{},
	}
	err := scheduler.CreateResourcePool("1", &PortResourcePool{
		Resource: "portReservation",
		PortBindingMapTCP: map[string]map[int64]string{
			"192.168.1.1": {},
			"192.168.1.2": {},
		},
		GhostMapTCP: map[string]map[int64]string{},
	})
	if err != nil {
		c.Fatal(err)
	}
	err = scheduler.CreateResourcePool("2", &PortResourcePool{
		Resource: "portReservation",
		PortBindingMapTCP: map[string]map[int64]string{
			"192.168.1.3": {},
			"192.168.1.4": {},
		},
	})
	if err != nil {
		c.Fatal(err)
	}

	id := 0
	specs := []PortSpec{{PublicPort: 8081, PrivatePort: 8081, Protocol: "tcp"}, {PublicPort: 8082, PrivatePort: 8082, Protocol: "tcp"}}
	data, err := scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: string(id), ResourceUUID: "u" + string(id), Resource: "portReservation", PortRequests: specs}}, nil)
	c.Assert(err, check.IsNil)
	c.Assert(data["portReservation"].([]map[string]interface{})[0]["allocatedIPs"], check.HasLen, 2)
	id++

	data, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: string(id), ResourceUUID: "u" + string(id), Resource: "portReservation", PortRequests: specs}}, nil)
	c.Assert(err, check.IsNil)
	c.Assert(data["portReservation"].([]map[string]interface{})[0]["allocatedIPs"], check.HasLen, 2)
	id++

	// the next action should error out, since there is no port available
	data, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: string(id), ResourceUUID: "u" + string(id), Resource: "portReservation", PortRequests: specs}}, nil)
	c.Assert(err, check.NotNil)
	id++

	releaseSpec := []PortSpec{{IPAddress: "192.168.1.1", PublicPort: 8081, PrivatePort: 8081, Protocol: "tcp"}, {IPAddress: "192.168.1.2", PublicPort: 8082, PrivatePort: 8082, Protocol: "tcp"}}
	err = scheduler.ReleaseResources("1", []ResourceRequest{PortBindingResourceRequest{InstanceID: string(id), ResourceUUID: "u" + string(id), Resource: "portReservation", PortRequests: releaseSpec}}, nil)
	c.Assert(err, check.IsNil)
	id++

	data, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: string(id), ResourceUUID: "u" + string(id), Resource: "portReservation", PortRequests: specs}}, nil)
	c.Assert(err, check.NotNil)
	id++

	// test roll back logic
	specs = []PortSpec{{PublicPort: 8083, PrivatePort: 8083, Protocol: "tcp"}, {PublicPort: 8081, PrivatePort: 8081, Protocol: "tcp"}, {PublicPort: 8081, PrivatePort: 8081, IPAddress: "192.168.1.2", Protocol: "tcp"}}
	data, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: string(id), ResourceUUID: "u" + string(id), Resource: "portReservation", PortRequests: specs}}, nil)
	c.Assert(err, check.NotNil)
	// assert that we still have 2 slots for 8083, bc it just rolled back
	c.Assert(getPortSlots(scheduler.hosts["1"].pools["portReservation"].(*PortResourcePool), 8083, "tcp"), check.Equals, 2)
	id++

	// test random port allocation
	specs = []PortSpec{{PrivatePort: 8083, Protocol: "tcp"}}
	data, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: string(id), ResourceUUID: "u" + string(id), Resource: "portReservation", PortRequests: specs}}, nil)
	port := data["portReservation"].([]map[string]interface{})[0]["allocatedIPs"].([]map[string]interface{})[0]["publicPort"].(int64)
	if port < 32768 || port > 61000 {
		c.Fatalf("Random Port Allocation failed. Port allocated %v", port)
	}
	id++

	// test 192.168.1.1:8084:8084, should only reserve port from 192.168.1.1
	specs = []PortSpec{{IPAddress: "192.168.1.1", PublicPort: 8084, PrivatePort: 8084, Protocol: "tcp"}}
	_, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: string(id), ResourceUUID: "u" + string(id), Resource: "portReservation", PortRequests: specs}}, nil)
	c.Assert(getPortSlots(scheduler.hosts["1"].pools["portReservation"].(*PortResourcePool), 8084, "tcp"), check.Equals, 1)
	id++

	// test 0.0.0.0:8084:8084, should reserve port on all ip4 address
	specs = []PortSpec{{IPAddress: "0.0.0.0", PublicPort: 8085, PrivatePort: 8085, Protocol: "tcp"}}
	_, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: string(id), ResourceUUID: "u" + string(id), Resource: "portReservation", PortRequests: specs}}, nil)
	c.Assert(getPortSlots(scheduler.hosts["1"].pools["portReservation"].(*PortResourcePool), 8085, "tcp"), check.Equals, 0)
	id++

	// test ghost map reservation
	specs = []PortSpec{{IPAddress: "192.168.1.5", PublicPort: 8085, PrivatePort: 8085, Protocol: "tcp"}}
	data, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: string(id), ResourceUUID: "u" + string(id), Resource: "portReservation", PortRequests: specs}}, nil)
	ip := data["portReservation"].([]map[string]interface{})[0]["allocatedIPs"].([]map[string]interface{})[0]["allocatedIP"].(string)
	c.Assert(ip, check.Equals, "192.168.1.5")
	c.Assert(err, check.IsNil)
	id++

	// should fail because we have reserved on ghost map
	specs = []PortSpec{{IPAddress: "192.168.1.5", PublicPort: 8085, PrivatePort: 8085, Protocol: "tcp"}}
	_, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: "2", ResourceUUID: "12346", Resource: "portReservation", PortRequests: specs}}, nil)
	c.Assert(err, check.NotNil)
}

func (s *SchedulerTestSuite) TestPortReservationProtocol(c *check.C) {
	scheduler := &Scheduler{
		hosts: map[string]*host{},
	}
	err := scheduler.CreateResourcePool("1", &PortResourcePool{
		Resource: "portReservation",
		PortBindingMapTCP: map[string]map[int64]string{
			"192.168.1.1": {},
			"192.168.1.2": {},
		},
		GhostMapTCP: map[string]map[int64]string{},
		PortBindingMapUDP: map[string]map[int64]string{
			"192.168.1.1": {},
			"192.168.1.2": {},
		},
		GhostMapUDP: map[string]map[int64]string{},
	})
	if err != nil {
		c.Fatal(err)
	}

	id := 0
	// reserve 8081 on 192.168.1.1 for tcp
	specs := []PortSpec{{IPAddress: "192.168.1.1", PublicPort: 8081, PrivatePort: 8081, Protocol: "tcp"}}
	data, err := scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: string(id), ResourceUUID: string(id), Resource: "portReservation", PortRequests: specs}}, nil)
	c.Assert(err, check.IsNil)
	c.Assert(data["portReservation"].([]map[string]interface{})[0]["allocatedIPs"], check.HasLen, 1)
	id++

	// reserve 8081 on 192.168.1.1 for udp, should succeed
	specs = []PortSpec{{IPAddress: "192.168.1.1", PublicPort: 8081, PrivatePort: 8081, Protocol: "udp"}}
	data, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: string(id), ResourceUUID: "u" + string(id), Resource: "portReservation", PortRequests: specs}}, nil)
	c.Assert(err, check.IsNil)
	c.Assert(data["portReservation"].([]map[string]interface{})[0]["allocatedIPs"], check.HasLen, 1)
	id++

	// should succeed on another ip
	specs = []PortSpec{{PublicPort: 8081, PrivatePort: 8081, Protocol: "udp"}, {PublicPort: 8081, PrivatePort: 8081, Protocol: "tcp"}}
	data, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: "2", ResourceUUID: "u" + string(id), Resource: "portReservation", PortRequests: specs}}, nil)
	c.Assert(err, check.IsNil)
	c.Assert(data["portReservation"].([]map[string]interface{})[0]["allocatedIPs"], check.HasLen, 2)
	id++

	// should fail
	specs = []PortSpec{{PublicPort: 8081, PrivatePort: 8081, Protocol: "udp"}}
	data, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: "3", ResourceUUID: "u" + string(id), Resource: "portReservation", PortRequests: specs}}, nil)
	c.Assert(err, check.NotNil)
	id++

	// release resource
	specs = []PortSpec{{PublicPort: 8081, PrivatePort: 8081, Protocol: "udp", IPAddress: "192.168.1.1"}}
	err = scheduler.ReleaseResources("1", []ResourceRequest{PortBindingResourceRequest{InstanceID: "2", ResourceUUID: "u" + string(id-3), Resource: "portReservation", PortRequests: specs}}, nil)
	c.Assert(err, check.IsNil)
	id++

	// should fail becase we only release udp
	specs = []PortSpec{{PublicPort: 8081, PrivatePort: 8081, Protocol: "tcp"}}
	data, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: string(id), ResourceUUID: "u" + string(id), Resource: "portReservation", PortRequests: specs}}, nil)
	c.Assert(err, check.NotNil)
	id++

	//should succeed
	specs = []PortSpec{{PublicPort: 8081, PrivatePort: 8081, Protocol: "udp"}}
	data, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: string(id), ResourceUUID: "u" + string(id), Resource: "portReservation", PortRequests: specs}}, nil)
	c.Assert(err, check.IsNil)
	id++

	// reserve tcp and udp at the same time
	specs = []PortSpec{{PublicPort: 8083, PrivatePort: 8083, Protocol: "udp"}, {PublicPort: 8083, PrivatePort: 8083, Protocol: "tcp"}}
	data, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: string(id), ResourceUUID: "u" + string(id), Resource: "portReservation", PortRequests: specs}}, nil)
	c.Assert(err, check.IsNil)
	// assert both return the same ip
	ip1 := data["portReservation"].([]map[string]interface{})[0]["allocatedIPs"].([]map[string]interface{})[0]["allocatedIP"].(string)
	ip2 := data["portReservation"].([]map[string]interface{})[0]["allocatedIPs"].([]map[string]interface{})[1]["allocatedIP"].(string)
	c.Assert(ip1, check.Equals, ip2)
	id++

	// reserve tcp on ghost map
	specs = []PortSpec{{PublicPort: 8083, PrivatePort: 8083, Protocol: "udp", IPAddress: "192.168.1.10"}}
	data, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: string(id), ResourceUUID: "u" + string(id), Resource: "portReservation", PortRequests: specs}}, nil)
	c.Assert(err, check.IsNil)
	id++

	// should still be able to reserve on tcp
	specs = []PortSpec{{PublicPort: 8083, PrivatePort: 8083, Protocol: "tcp", IPAddress: "192.168.1.10"}}
	data, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: string(id), ResourceUUID: "u" + string(id), Resource: "portReservation", PortRequests: specs}}, nil)
	c.Assert(err, check.IsNil)
	id++

	// release port on ghost map
	specs = []PortSpec{{PublicPort: 8083, PrivatePort: 8083, Protocol: "tcp", IPAddress: "192.168.1.10"}}
	err = scheduler.ReleaseResources("1", []ResourceRequest{PortBindingResourceRequest{InstanceID: string(id - 1), ResourceUUID: "u" + string(id-1), Resource: "portReservation", PortRequests: specs}}, nil)
	c.Assert(err, check.IsNil)
	id++

	// reserve it again, should succeed
	specs = []PortSpec{{PublicPort: 8083, PrivatePort: 8083, Protocol: "tcp", IPAddress: "192.168.1.10"}}
	data, err = scheduler.ReserveResources("1", false, []ResourceRequest{PortBindingResourceRequest{InstanceID: string(id), ResourceUUID: "u" + string(id), Resource: "portReservation", PortRequests: specs}}, nil)
	c.Assert(err, check.IsNil)
}

func (s *SchedulerTestSuite) TestRaceConditionPrioritize(c *check.C) {
	scheduler := NewScheduler(1)

	err := scheduler.CreateResourcePool("1", &ComputeResourcePool{
		Resource: "instanceReservation",
		Used:     0,
		Total:    500,
	})
	if err != nil {
		c.Fatal(err)
	}
	err = scheduler.CreateResourcePool("2", &ComputeResourcePool{
		Resource: "instanceReservation",
		Used:     0,
		Total:    500,
	})
	if err != nil {
		c.Fatal(err)
	}

	tests := []rezTest{
		{"1", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "instanceReservation"}}, []string{"1", "2"}},

		{"2", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "instanceReservation"}}, []string{"2", "1"}},

		{"3", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "instanceReservation"}}, []string{"1", "2"}},

		{"4", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "instanceReservation"}}, []string{"2", "1"}},

		{"5", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "instanceReservation"}}, []string{"1", "2"}},

		{"6", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "instanceReservation"}}, []string{"2", "1"}},

		{"7", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "instanceReservation"}}, []string{"1", "2"}},

		{"8", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "instanceReservation"}}, []string{"2", "1"}},

		{"9", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "instanceReservation"}}, []string{"1", "2"}},

		{"10", false, []ResourceRequest{AmountBasedResourceRequest{Amount: 1, Resource: "instanceReservation"}}, []string{"2", "1"}},
	}
	checkOnlyPrioritization(scheduler, tests, c)
}

func (s *SchedulerTestSuite) TestReservePortsWithGhostIPEntry(c *check.C) {
	scheduler := &Scheduler{
		hosts: map[string]*host{},
	}
	err := scheduler.CreateResourcePool("1", &PortResourcePool{
		Resource: "portReservation",
		PortBindingMapTCP: map[string]map[int64]string{
			"0.0.0.0": {},
		},
		GhostMapTCP: map[string]map[int64]string{
			"192.168.1.1": {
				80: "12345",
			},
		},
		PortBindingMapUDP: map[string]map[int64]string{},
		GhostMapUDP:       map[string]map[int64]string{},
	})
	if err != nil {
		c.Fatal(err)
	}
	specs := []PortSpec{{PublicPort: 80, PrivatePort: 80, Protocol: "tcp"}}
	rrequests := []ResourceRequest{PortBindingResourceRequest{InstanceID: "1", ResourceUUID: "12", Resource: "portReservation", PortRequests: specs}}
	result, err := scheduler.PrioritizeCandidates(rrequests, Context{})
	c.Assert(result, check.HasLen, 0)

	_, err = scheduler.ReserveResources("1", false, rrequests, nil)
	c.Assert(err, check.NotNil)
	c.Assert(err, check.ErrorMatches, ".*Port is used by IP 192.168.1.1.*")
}

func (s *SchedulerTestSuite) TestReleaseNoop(c *check.C) {
	scheduler := &Scheduler{
		hosts: map[string]*host{},
	}
	err := scheduler.CreateResourcePool("1", &PortResourcePool{
		Resource: "portReservation",
		PortBindingMapTCP: map[string]map[int64]string{
			"0.0.0.0": {},
		},
		GhostMapTCP:       map[string]map[int64]string{},
		PortBindingMapUDP: map[string]map[int64]string{},
		GhostMapUDP:       map[string]map[int64]string{},
	})
	if err != nil {
		c.Fatal(err)
	}
	// container A sends 80:80 requests
	specs := []PortSpec{{PublicPort: 80, PrivatePort: 80, Protocol: "tcp"}}
	r1 := []ResourceRequest{PortBindingResourceRequest{InstanceID: "1", ResourceUUID: "12", Resource: "portReservation", PortRequests: specs}}
	_, err = scheduler.ReserveResources("1", false, r1, nil)
	c.Assert(err, check.IsNil)

	// release A
	specs = []PortSpec{{IPAddress: "0.0.0.0", PublicPort: 80, PrivatePort: 80, Protocol: "tcp"}}
	r2 := []ResourceRequest{PortBindingResourceRequest{InstanceID: "1", ResourceUUID: "12", Resource: "portReservation", PortRequests: specs}}
	err = scheduler.ReleaseResources("1", r2, nil)
	c.Assert(err, check.IsNil)

	// container B sends 80:80
	specs = []PortSpec{{PublicPort: 80, PrivatePort: 80, Protocol: "tcp"}}
	r3 := []ResourceRequest{PortBindingResourceRequest{InstanceID: "2", ResourceUUID: "13", Resource: "portReservation", PortRequests: specs}}
	_, err = scheduler.ReserveResources("1", false, r3, nil)
	c.Assert(err, check.IsNil)

	// release A
	err = scheduler.ReleaseResources("1", r2, nil)
	c.Assert(err, check.IsNil)

	// B is not release so we still don't have port 80 available
	c.Assert(getPortSlots(scheduler.hosts["1"].pools["portReservation"].(*PortResourcePool), 80, "tcp"), check.Equals, 0)
}

// TestPortCornerCase test two conner cases:
// 1. Host Label 0.0.0.0, container A use 0.0.0.0:8080:8080, container B use 192.168.1.1:8080:8080, should fail.
// 2. Host Label 192.168.1.1, 192.168.1.2, Container A use 0.0.0.0:8080:8080, container B use 192.168.1.3:8080:8080, should fail.
func (s *SchedulerTestSuite) TestPortCornerCase(c *check.C) {
	scheduler := &Scheduler{
		hosts: map[string]*host{},
	}
	err := scheduler.CreateResourcePool("1", &PortResourcePool{
		Resource: "portReservation",
		PortBindingMapTCP: map[string]map[int64]string{
			"0.0.0.0": {8080: "12345"},
		},
		GhostMapTCP:       map[string]map[int64]string{},
		PortBindingMapUDP: map[string]map[int64]string{},
		GhostMapUDP:       map[string]map[int64]string{},
	})
	if err != nil {
		c.Fatal(err)
	}
	err = scheduler.CreateResourcePool("2", &PortResourcePool{
		Resource: "portReservation",
		PortBindingMapTCP: map[string]map[int64]string{
			"192.168.1.1": {},
			"192.168.1.2": {},
		},
		GhostMapTCP: map[string]map[int64]string{
			"0.0.0.0": {},
		},
		PortBindingMapUDP: map[string]map[int64]string{},
		GhostMapUDP:       map[string]map[int64]string{},
	})
	if err != nil {
		c.Fatal(err)
	}

	specs := []PortSpec{{IPAddress: "192.168.1.1", PublicPort: 8080, PrivatePort: 8080, Protocol: "tcp"}}
	r1 := []ResourceRequest{PortBindingResourceRequest{InstanceID: "1", ResourceUUID: "12", Resource: "portReservation", PortRequests: specs}}
	_, err = scheduler.ReserveResources("1", false, r1, nil)
	c.Assert(err, check.NotNil)

	// reserve 0.0.0.0 on host 2, both ip 192.168.1.1 and 192.168.1.2 should be reserved on 8080
	specs = []PortSpec{{IPAddress: "0.0.0.0", PublicPort: 8080, PrivatePort: 8080, Protocol: "tcp"}}
	r2 := []ResourceRequest{PortBindingResourceRequest{InstanceID: "2", ResourceUUID: "123", Resource: "portReservation", PortRequests: specs}}
	_, err = scheduler.ReserveResources("2", false, r2, nil)
	c.Assert(err, check.IsNil)
	c.Assert(getPortSlots(scheduler.hosts["2"].pools["portReservation"].(*PortResourcePool), 8080, "tcp"), check.Equals, 0)

	//reserve 192.168.1.3:8080:8080 should fail
	specs = []PortSpec{{IPAddress: "192.168.1.3", PublicPort: 8080, PrivatePort: 8080, Protocol: "tcp"}}
	r3 := []ResourceRequest{PortBindingResourceRequest{InstanceID: "3", ResourceUUID: "124", Resource: "portReservation", PortRequests: specs}}
	_, err = scheduler.ReserveResources("2", false, r3, nil)
	c.Assert(err, check.NotNil)

	// release 0.0.0.0 should release all the pools
	err = scheduler.ReleaseResources("2", r2, nil)
	c.Assert(err, check.IsNil)
	c.Assert(getPortSlots(scheduler.hosts["2"].pools["portReservation"].(*PortResourcePool), 8080, "tcp"), check.Equals, 2)
}

func (s *SchedulerTestSuite) TestMixPortRequests(c *check.C) {
	scheduler := &Scheduler{
		hosts: map[string]*host{},
	}
	err := scheduler.CreateResourcePool("1", &PortResourcePool{
		Resource: "portReservation",
		PortBindingMapTCP: map[string]map[int64]string{
			"0.0.0.0": {},
		},
		GhostMapTCP:       map[string]map[int64]string{},
		PortBindingMapUDP: map[string]map[int64]string{},
		GhostMapUDP:       map[string]map[int64]string{},
	})
	if err != nil {
		c.Fatal(err)
	}
	err = scheduler.CreateResourcePool("2", &PortResourcePool{
		Resource: "portReservation",
		PortBindingMapTCP: map[string]map[int64]string{
			"192.168.1.1": {8080: "12345"},
			"192.168.1.2": {},
			"192.168.1.3": {},
			"192.168.1.4": {},
			"192.168.1.5": {},
		},
		GhostMapTCP: map[string]map[int64]string{
			"0.0.0.0": {},
		},
		PortBindingMapUDP: map[string]map[int64]string{},
		GhostMapUDP:       map[string]map[int64]string{},
	})
	if err != nil {
		c.Fatal(err)
	}

	specs := []PortSpec{{IPAddress: "192.168.1.2", PublicPort: 8080, PrivatePort: 8080, Protocol: "tcp"}, {PublicPort: 8081, PrivatePort: 8081, Protocol: "tcp"}}
	r1 := []ResourceRequest{PortBindingResourceRequest{InstanceID: "1", ResourceUUID: "12", Resource: "portReservation", PortRequests: specs}}
	hosts, err := scheduler.PrioritizeCandidates(r1, Context{})
	c.Assert(err, check.IsNil)
	c.Assert(hosts, check.DeepEquals, []string{"2"})

	specs = []PortSpec{{IPAddress: "192.168.1.3", PublicPort: 8080, PrivatePort: 8080, Protocol: "tcp"}, {PublicPort: 8081, PrivatePort: 8081, Protocol: "tcp"}}
	r2 := []ResourceRequest{PortBindingResourceRequest{InstanceID: "1", ResourceUUID: "12", Resource: "portReservation", PortRequests: specs}}
	hosts, err = scheduler.PrioritizeCandidates(r2, Context{})
	c.Assert(err, check.IsNil)
	c.Assert(hosts, check.DeepEquals, []string{"2"})

	specs = []PortSpec{{IPAddress: "192.168.1.4", PublicPort: 8080, PrivatePort: 8080, Protocol: "tcp"}, {PublicPort: 8081, PrivatePort: 8081, Protocol: "tcp"}}
	r3 := []ResourceRequest{PortBindingResourceRequest{InstanceID: "1", ResourceUUID: "12", Resource: "portReservation", PortRequests: specs}}
	hosts, err = scheduler.PrioritizeCandidates(r3, Context{})
	c.Assert(err, check.IsNil)
	c.Assert(hosts, check.DeepEquals, []string{"2"})

	specs = []PortSpec{{IPAddress: "192.168.1.5", PublicPort: 8080, PrivatePort: 8080, Protocol: "tcp"}, {PublicPort: 8081, PrivatePort: 8081, Protocol: "tcp"}}
	r4 := []ResourceRequest{PortBindingResourceRequest{InstanceID: "1", ResourceUUID: "12", Resource: "portReservation", PortRequests: specs}}
	hosts, err = scheduler.PrioritizeCandidates(r4, Context{})
	c.Assert(err, check.IsNil)
	c.Assert(hosts, check.DeepEquals, []string{"2"})

	specs = []PortSpec{{IPAddress: "192.168.1.1", PublicPort: 8080, PrivatePort: 8080, Protocol: "tcp"}, {PublicPort: 8081, PrivatePort: 8081, Protocol: "tcp"}}
	r5 := []ResourceRequest{PortBindingResourceRequest{InstanceID: "1", ResourceUUID: "12", Resource: "portReservation", PortRequests: specs}}
	hosts, err = scheduler.PrioritizeCandidates(r5, Context{})
	c.Assert(err, check.IsNil)
	c.Assert(hosts, check.DeepEquals, []string{})
}

func getPortSlots(pool *PortResourcePool, port int64, protocol string) int {
	if protocol == "tcp" {
		slots := 0
		for _, portMap := range pool.PortBindingMapTCP {
			if portMap[port] == "" {
				slots++
			}
		}
		return slots
	}
	slots := 0
	for _, portMap := range pool.PortBindingMapUDP {
		if portMap[port] == "" {
			slots++
		}
	}
	return slots

}

func checkOnlyPrioritization(scheduler *Scheduler, tests []rezTest, c *check.C) {
	for _, t := range tests {
		_, err := scheduler.PrioritizeCandidates(t.resourceRequests, Context{})
		if err != nil {
			c.Fatal(err)
		}
		c.Logf("Checking %v", t.id)
	}
	scheduler.mu.RLock()
	c.Assert(scheduler.hosts["1"].pools["instanceReservation"].(*ComputeResourcePool).Used, check.Equals, scheduler.hosts["2"].pools["instanceReservation"].(*ComputeResourcePool).Used)
	scheduler.mu.RUnlock()
}

func checkPrioritizationAndReserve(scheduler *Scheduler, tests []rezTest, c *check.C) {
	for _, t := range tests {
		actual, err := scheduler.PrioritizeCandidates(t.resourceRequests, Context{})
		if err != nil {
			c.Fatal(err)
		}
		c.Logf("Checking %v", t.id)
		c.Assert(actual, check.DeepEquals, t.expected)

		if len(t.expected) > 0 {
			_, err = scheduler.ReserveResources(t.expected[0], t.force, t.resourceRequests, nil)
			c.Assert(err, check.IsNil)
		}
	}
}
