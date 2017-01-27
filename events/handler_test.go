package events

import (
	"testing"

	check "gopkg.in/check.v1"

	revents "github.com/rancher/event-subscriber/events"
	"github.com/rancher/go-rancher/client"
	"github.com/rancher/scheduler/scheduler"
)

// gocheck setup
func Test(t *testing.T) { check.TestingT(t) }

type MetadataTestSuite struct{}

var _ = check.Suite(&MetadataTestSuite{})

func (s *MetadataTestSuite) SetUpSuite(c *check.C) {
	// Nothing to setup yet
}

func (s *MetadataTestSuite) TestPrioritizeEvent(c *check.C) {
	sched := scheduler.NewScheduler(-1)

	sched.CreateResourcePool("1", &scheduler.ComputeResourcePool{
		Resource: "memory",
		Total:    1,
		Used:     0,
	})
	handler := &schedulingHandler{
		scheduler: sched,
	}

	req := map[string]interface{}{
		"phase": "instance.allocate",
		"resourceRequests": []interface{}{
			map[string]interface{}{
				"type":     "computePool",
				"resource": "memory",
				"amount":   1,
			},
		},
	}
	event := &revents.Event{
		Data: map[string]interface{}{
			"schedulerRequest": req,
		},
	}

	mockPublishOps := &mockPublish{}
	mockClient := &client.RancherClient{
		Publish: mockPublishOps,
	}

	handler.Prioritize(event, mockClient)
	c.Assert(mockPublishOps.published.Data, check.DeepEquals, map[string]interface{}{"prioritizedCandidates": []string{"1"}})

	req["hostID"] = "1"
	err := handler.Reserve(event, mockClient)
	c.Assert(err, check.IsNil)
	// data is no longer nil because it needs to return data to cattle
	// c.Assert(mockPublishOps.published.Data, check.IsNil)

	err = handler.Prioritize(event, mockClient)
	c.Assert(err, check.IsNil)
	c.Assert(mockPublishOps.published.Data, check.DeepEquals, map[string]interface{}{"prioritizedCandidates": []string{}})

	err = handler.Reserve(event, mockClient)
	c.Assert(err, check.ErrorMatches, ".*memory.*")

	req["force"] = true
	err = handler.Reserve(event, mockClient)
	c.Assert(err, check.IsNil)
	//c.Assert(mockPublishOps.published.Data, check.IsNil)

	err = handler.Release(event, mockClient)
	c.Assert(err, check.IsNil)
	c.Assert(mockPublishOps.published.Data, check.IsNil)
	err = handler.Release(event, mockClient)
	c.Assert(err, check.IsNil)

	err = handler.Prioritize(event, mockClient)
	c.Assert(err, check.IsNil)
	c.Assert(mockPublishOps.published.Data, check.DeepEquals, map[string]interface{}{"prioritizedCandidates": []string{"1"}})
}

func (s *MetadataTestSuite) TestProcessPhase(c *check.C) {
	sched := scheduler.NewScheduler(-1)

	sched.CreateResourcePool("1", &scheduler.PortResourcePool{
		Resource: "portReservation",
		PortBindingMapTCP: map[string]map[int64]string{
			"192.168.1.1": {},
			"192.168.1.2": {},
		},
		GhostMapTCP: map[string]map[int64]string{
			"192.168.1.3": {},
			"192.168.1.4": {},
		},
		PortBindingMapUDP: map[string]map[int64]string{
			"192.168.1.1": {},
			"192.168.1.2": {},
		},
		GhostMapUDP: map[string]map[int64]string{
			"192.168.1.3": {},
			"192.168.1.4": {},
		},
	})
	handler := &schedulingHandler{
		scheduler: sched,
	}

	mockPublishOps := &mockPublish{}
	mockClient := &client.RancherClient{
		Publish: mockPublishOps,
	}

	req := map[string]interface{}{
		"phase": "instance.allocate",
		"resourceRequests": []interface{}{
			map[string]interface{}{
				"type":         "portPool",
				"resource":     "portReservation",
				"instanceID":   "1",
				"instanceUUID": "12345",
				"portRequests": []map[string]interface{}{
					{
						"ipAddress":   "192.168.1.1",
						"privatePort": 8080,
						"publicPort":  8080,
						"protocol":    "tcp",
					},
				},
			},
		},
		"hostID": "1",
	}

	req2 := map[string]interface{}{
		"phase": "instance.allocate",
		"resourceRequests": []interface{}{
			map[string]interface{}{
				"type":         "portPool",
				"resource":     "portReservation",
				"instanceID":   "2",
				"instanceUUID": "12346",
				"portRequests": []map[string]interface{}{
					{
						"ipAddress":   "192.168.1.1",
						"privatePort": 8080,
						"publicPort":  8080,
						"protocol":    "tcp",
					},
				},
			},
		},
		"hostID": "1",
	}

	event := &revents.Event{
		Data: map[string]interface{}{
			"schedulerRequest": req,
		},
	}

	event2 := &revents.Event{
		Data: map[string]interface{}{
			"schedulerRequest": req2,
		},
	}

	// reserve first time, it should reserve as normal
	err := handler.Reserve(event, mockClient)
	c.Assert(err, check.IsNil)

	req["phase"] = "instance.start"
	//set phase to instance.start, then should trigger the logic and pass
	err = handler.Reserve(event, mockClient)
	c.Assert(err, check.IsNil)

	req["phase"] = "instance.start"
	//set phase to instance.start, then should also pass since it is the same instance
	err = handler.Reserve(event, mockClient)
	c.Assert(err, check.IsNil)

	// release port by instance.stop
	req["phase"] = "instance.stop"
	err = handler.Release(event, mockClient)
	c.Assert(err, check.IsNil)

	// test if port is release by sending another event
	err = handler.Reserve(event2, mockClient)
	c.Assert(err, check.IsNil)

	// restart the first one, should not pass because it is used by another instance
	req["phase"] = "instance.start"
	err = handler.Reserve(event, mockClient)
	c.Assert(err, check.NotNil)
}

type mockPublish struct {
	client.PublishOperations
	published *client.Publish
}

func (m *mockPublish) Create(publish *client.Publish) (*client.Publish, error) {
	m.published = publish
	return publish, nil
}
