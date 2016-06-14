package service

import "github.com/rancher/go-rancher/client"

//SchedulerError structure contains the error resource definition
type SchedulerError struct {
	client.Resource
	Status  string `json:"status"`
	Message string `json:"message"`
}
