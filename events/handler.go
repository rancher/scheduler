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

const (
	computePool = "computePool"
	portPool    = "portPool"
)

type schedulingHandler struct {
	scheduler *scheduler.Scheduler
}

func (h *schedulingHandler) Reserve(event *revents.Event, client *client.RancherClient) error {
	data, err := getEventData(event)
	if err != nil {
		return errors.Wrapf(err, "Error decoding reserve event %v.", event)
	}

	result, err := h.scheduler.ReserveResources(data.HostID, data.Force, data.ResourceRequests)
	if err != nil {
		return errors.Wrapf(err, "Error reserving resources. Event: %v.", event)
	}

	return publish(event, result, client)
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
	logrus.Infof("Received event: Name: %s, Event Id: %s, ResourceId : %v", event.Name, event.ID, event.ResourceID)
	return decodeEvent(event, "schedulerRequest")
}

// decodeEvent decodes the request from cattle into ResourceRequest Type
func decodeEvent(event *revents.Event, key string) (*schedulerData, error) {
	result := &schedulerData{}
	result.ResourceRequests = []scheduler.ResourceRequest{}
	phase, ok := event.Data[key].(map[string]interface{})["phase"].(string)
	if !ok {
		phase = ""
	}
	if s, ok := event.Data[key]; ok {
		if resourceRequests, ok := s.(map[string]interface{})["resourceRequests"]; ok {
			for _, request := range resourceRequests.([]interface{}) {
				baseRequests := scheduler.BaseResourceRequest{}
				err := mapstructure.Decode(request, &baseRequests)
				if err != nil {
					return nil, err
				}
				switch baseRequests.Type {
				case computePool:
					computeRequest := scheduler.AmountBasedResourceRequest{}
					err := mapstructure.Decode(request, &computeRequest)
					if err != nil {
						return nil, err
					}
					if phase == "instance.allocate" || phase == "instance.deallocate" {
						result.ResourceRequests = append(result.ResourceRequests, computeRequest)
					}
				case portPool:
					portRequest := scheduler.PortBindingResourceRequest{}
					err := mapstructure.Decode(request, &portRequest)
					if err != nil {
						return nil, err
					}
					result.ResourceRequests = append(result.ResourceRequests, portRequest)
				}
			}
		}
		if hostID, ok := s.(map[string]interface{})["hostID"]; ok {
			result.HostID = hostID.(string)
		}
		if force, ok := s.(map[string]interface{})["force"]; ok {
			result.Force = force.(bool)
		}
		return result, nil
	}
	return nil, fmt.Errorf("Event doesn't contain %v data. Event: %#v", key, event)
}

type schedulerData struct {
	HostID           string `mapstructure:"hostId"`
	Force            bool
	ResourceRequests []scheduler.ResourceRequest
}
