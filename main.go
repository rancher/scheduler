package main

import (
	"fmt"
	"net/http"
	log "github.com/Sirupsen/logrus"
	"github.com/rancher/scheduler/service"
)

func main() {
	log.Infof("Starting Rancher Scheduler service")
	router := service.NewRouter()
	handler := service.MuxWrapper{true, router}

	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", 8090), &handler))
}
