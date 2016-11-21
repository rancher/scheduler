package events

import (
	"fmt"

	"github.com/Sirupsen/logrus"
	"github.com/mitchellh/mapstructure"
	"github.com/pkg/errors"
	revents "github.com/rancher/event-subscriber/events"
	"github.com/rancher/go-rancher/client"
	"github.com/rancher/scheduler/scheduler"
)

type schedulingHandler struct {
	scheduler *scheduler.Scheduler
}

func (h *schedulingHandler) Reserve(event *revents.Event, client *client.RancherClient) error {
	data, err := getEventData(event)
	if err != nil {
		return errors.Wrapf(err, "Error decoding reserve event %v.", event)
	}

	err = h.scheduler.ReserveResources(data.HostID, data.Force, data.ResourceRequests)
	if err != nil {
		return errors.Wrapf(err, "Error reserving resources. Event: %v.", event)
	}

	return publish(event, nil, client)
}

func (h *schedulingHandler) Release(event *revents.Event, client *client.RancherClient) error {
	data, err := getEventData(event)
	if err != nil {
		return errors.Wrapf(err, "Error decoding release event %v.", event)
	}

	err = h.scheduler.ReleaseResources(data.HostID, data.ResourceRequests)
	if err != nil {
		return errors.Wrapf(err, "Error releasing resources. Event %v.", event)
	}

	return publish(event, nil, client)
}

func (h *schedulingHandler) Prioritize(event *revents.Event, client *client.RancherClient) error {
	data, err := getEventData(event)
	if err != nil {
		return errors.Wrapf(err, "Error decoding prioritize event %v.", event)
	}

	candidates, err := h.scheduler.PrioritizeCandidates(data.ResourceRequests)
	if err != nil {
		return errors.Wrapf(err, "Error prioritizing candidates. Event %v", event)
	}

	eventDataWrapper := map[string]interface{}{"prioritizedCandidates": candidates}
	return publish(event, eventDataWrapper, client)
}

func publish(event *revents.Event, data map[string]interface{}, apiClient *client.RancherClient) error {
	reply := &client.Publish{
		Name:        event.ReplyTo,
		PreviousIds: []string{event.ID},
	}
	reply.ResourceType = "schedulerRequest"
	reply.ResourceId = event.ResourceID
	reply.Data = data

	logrus.Infof("Reply: Name: %v, PreviousIds: %v, ResourceId: %v, Data: %v.", reply.Name, reply.PreviousIds, reply.ResourceId, reply.Data)
	_, err := apiClient.Publish.Create(reply)
	return err
}

func getEventData(event *revents.Event) (*schedulerData, error) {
	logrus.Infof("Received event: Name: %s, Event Id: %s, Resource Id: %s", event.Name, event.ID, event.ResourceID)
	data := &schedulerData{}
	err := decodeEvent(event, "schedulerRequest", data)
	return data, err
}

func decodeEvent(event *revents.Event, key string, target interface{}) error {
	if s, ok := event.Data[key]; ok {
		err := mapstructure.Decode(s, target)
		return err
	}
	return fmt.Errorf("Event doesn't contain %v data. Event: %#v", key, event)
}

type schedulerData struct {
	HostID           string `mapstructure:"hostId"`
	Force            bool
	ResourceRequests []scheduler.ResourceRequest
}
