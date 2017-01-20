package events

import (
	"testing"

	revents "github.com/rancher/event-subscriber/events"
	"github.com/rancher/go-rancher/client"
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

func (s *MetadataTestSuite) TestPrioritizeEvent(c *check.C) {
	sched := scheduler.NewScheduler()
	sched.CreateResourcePool("1", &scheduler.ComputeResourcePool{
		Resource: "memory",
		Total:    1,
		Used:     0,
	})
	handler := &schedulingHandler{
		scheduler: sched,
	}

	req := map[string]interface{}{
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

type mockPublish struct {
	client.PublishOperations
	published *client.Publish
}

func (m *mockPublish) Create(publish *client.Publish) (*client.Publish, error) {
	m.published = publish
	return publish, nil
}
