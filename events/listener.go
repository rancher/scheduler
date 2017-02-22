package events

import (
	"github.com/Sirupsen/logrus"
	revents "github.com/rancher/event-subscriber/events"
	"github.com/rancher/event-subscriber/locks"
	"github.com/rancher/go-rancher/v2"
	"github.com/rancher/scheduler/scheduler"
)

func ConnectToEventStream(cattleURL, accessKey, secretKey string, scheduler *scheduler.Scheduler) error {
	logrus.Info("Connecting to cattle event stream.")
	handler := &schedulingHandler{
		scheduler: scheduler,
	}

	eventHandlers := map[string]revents.EventHandler{
		"scheduler.prioritize": handler.Prioritize,
		"scheduler.reserve":    handler.Reserve,
		"scheduler.release":    handler.Release,
		"ping":                 func(_ *revents.Event, _ *client.RancherClient) error { return nil },
	}

	router, err := revents.NewEventRouter("", 0, cattleURL, accessKey, secretKey, nil, eventHandlers, "", 100, revents.DefaultPingConfig)
	if err != nil {
		return err
	}

	wp := revents.SkippingWorkerPool(100, nopLocker)
	err = router.RunWithWorkerPool(wp)
	return err
}

func nopLocker(_ *revents.Event) locks.Locker { return locks.NopLocker() }
