package main

import (
	"fmt"
	"net/http"
	"os"

	"strconv"

	"github.com/Sirupsen/logrus"
	"github.com/pkg/errors"
	"github.com/rancher/go-rancher-metadata/metadata"
	"github.com/rancher/scheduler/events"
	"github.com/rancher/scheduler/resourcewatchers"
	"github.com/rancher/scheduler/scheduler"
	"github.com/urfave/cli"
)

var VERSION = "v0.1.0-dev"

func init() {
	logrus.SetOutput(os.Stdout)
}

func main() {
	app := cli.NewApp()
	app.Name = "scheduler"
	app.Version = VERSION
	app.Usage = "An external resource based scheduler for Rancher."
	app.Action = run
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:  "metadata-address",
			Usage: "The metadata service address",
			Value: "rancher-metadata",
		},
		cli.IntFlag{
			Name:  "health-check-port",
			Usage: "Port to listen on for healthchecks",
			Value: 80,
		},
	}

	app.Run(os.Args)
}

func run(c *cli.Context) error {
	if os.Getenv("RANCHER_DEBUG") == "true" {
		logrus.SetLevel(logrus.DebugLevel)
	}

	sleep := os.Getenv("CATTLE_SCHEDULER_SLEEPTIME")
	time := 1
	if sleep != "" {
		if val, err := strconv.Atoi(sleep); err != nil {
			time = val
		}
	}
	scheduler := scheduler.NewScheduler(time)
	mdClient := metadata.NewClient(fmt.Sprintf("http://%s/2016-07-29", c.String("metadata-address")))

	url := os.Getenv("CATTLE_URL")
	ak := os.Getenv("CATTLE_ACCESS_KEY")
	sk := os.Getenv("CATTLE_SECRET_KEY")
	if url == "" || ak == "" || sk == "" {
		logrus.Fatalf("Cattle connection environment variables not available. URL: %v, access key %v, secret key redacted.", url, ak)
	}

	exit := make(chan error)
	go func(exit chan<- error) {
		err := events.ConnectToEventStream(url, ak, sk, scheduler)
		exit <- errors.Wrapf(err, "Cattle event subscriber exited.")
	}(exit)

	go func(exit chan<- error) {
		err := resourcewatchers.WatchMetadata(mdClient, scheduler)
		exit <- errors.Wrap(err, "Metadata watcher exited")
	}(exit)

	go func(exit chan<- error) {
		err := startHealthCheck(c.Int("health-check-port"))
		exit <- errors.Wrapf(err, "Healthcheck provider died.")
	}(exit)

	err := <-exit
	logrus.Errorf("Exiting scheduler with error: %v", err)
	return err
}

func startHealthCheck(listen int) error {
	http.HandleFunc("/healthcheck", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, "ok")
	})
	logrus.Infof("Listening for health checks on 0.0.0.0:%d/healthcheck", listen)
	err := http.ListenAndServe(fmt.Sprintf(":%d", listen), nil)
	return err
}
