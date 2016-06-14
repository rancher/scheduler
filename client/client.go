package client

import (
	"fmt"
	log "github.com/Sirupsen/logrus"
	"github.com/rancher/go-rancher/client"
	"os"
)

var (
	rancherClient *client.RancherClient
)

// GetHosts: get hosts from cattle
func getRancherClient() (*client.RancherClient, error) {
	if rancherClient != nil {
		return rancherClient, nil
	}
	cattleURL := os.Getenv("CATTLE_URL")
	if len(cattleURL) == 0 {
		log.Info("getRancherClient: CATTLE_URL is not set")
		return nil, nil
	}

	cattleAccessKey := os.Getenv("CATTLE_ACCESS_KEY")
	if len(cattleAccessKey) == 0 {
		log.Info("getRancherClient: CATTLE_ACCESS_KEY is not set")
		return nil, nil
	}

	cattleSecretKey := os.Getenv("CATTLE_SECRET_KEY")
	if len(cattleSecretKey) == 0 {
		log.Info("getRancherClient: CATTLE_SECRET_KEY is not set")
		return nil, nil
	}

	opts := &client.ClientOpts{
		Url:       cattleURL,
		AccessKey: cattleAccessKey,
		SecretKey: cattleSecretKey,
	}

	var err error = nil
	rancherClient, err = client.NewRancherClient(opts)

	if err != nil {
		log.Fatalf("Failed to create Rancher client %v", err)
		rancherClient = nil
	}
	return rancherClient, err
}

// GetHost: get host data from cattle
func GetHost(hostId string) (*client.Host, error) {
	clientPtr, err := getRancherClient()
	if err != nil {
		log.Fatalf("Failed to get Rancher client %v", err)
		return nil, err
	}
	host, err := clientPtr.Host.ById(hostId)
	if err != nil {
		return nil, fmt.Errorf("Coudln't get host with id:%s. Error: %#v", hostId, err)
	}

	return host, nil
}

// GetInstance: get instance data from cattle
func GetInstance(instanceId string) (*client.Instance, error) {
	clientPtr, err := getRancherClient()
	if err != nil {
		log.Fatalf("Failed to get Rancher client %v", err)
		return nil, err
	}
	instance, err := clientPtr.Instance.ById(instanceId)
	if err != nil {
		return nil, fmt.Errorf("Coudln't get instance with id:%s. Error: %#v", instanceId, err)
	}

	return instance, nil
}

// GetInstance: get instance data from cattle
func GetContainer(containerId string) (*client.Container, error) {
	clientPtr, err := getRancherClient()
	if err != nil {
		log.Fatalf("Failed to get Rancher client %v", err)
		return nil, err
	}
	container, err := clientPtr.Container.ById(containerId)
	if err != nil {
		return nil, fmt.Errorf("Coudln't get container with id:%s. Error: %#v", containerId, err)
	}

	return container, nil
}

// GetContainersOnHost: get all containers for a host from cattle
func GetContainersOnHost(hostId string, envId string) ([]client.Container, error) {
	clientPtr, err := getRancherClient()
	if err != nil {
		log.Fatalf("Failed to get Rancher client %v", err)
		return nil, err
	}
	opts := client.NewListOpts()
	opts.Filters["accountId"] = envId
	opts.Filters["allocationState"] = "active"

	containerCollection, err := clientPtr.Container.List(opts)
	if err != nil {
		return nil, fmt.Errorf("Coudln't get container list on host by id [%s]. Error: %#v",
			hostId, err)
	}

	// filters by hostId, which opts does not support
	containerList := make([]client.Container, 0)
	for _, container := range containerCollection.Data {
		if container.HostId == hostId {
			containerList = append(containerList, container)
		}
	}
	log.Infof("got all the containers with hostId %s", hostId)
	return containerList, nil
}

// GetInstance: get instance data from cattle
func GetVM(instanceId string) (*client.VirtualMachine, error) {
	clientPtr, err := getRancherClient()
	if err != nil {
		log.Fatalf("Failed to get Rancher client %v", err)
		return nil, err
	}
	vm, err := clientPtr.VirtualMachine.ById(instanceId)
	if err != nil {
		return nil, fmt.Errorf("Coudln't get vm instance with id:%s. Error: %#v", instanceId, err)
	}

	return vm, nil
}

// GetInstancesOnHost: get all instances for a host from cattle
func GetVMsOnHost(hostId string, envId string) ([]client.VirtualMachine, error) {
	clientPtr, err := getRancherClient()
	if err != nil {
		log.Fatalf("Failed to get Rancher client %v", err)
		return nil, err
	}
	opts := client.NewListOpts()
	opts.Filters["accountId"] = envId
	opts.Filters["allocationState"] = "active"

	vmCollection, err := clientPtr.VirtualMachine.List(opts)
	if err != nil {
		return nil, fmt.Errorf("Coudln't get vm instance list on host by id [%s]. Error: %#v",
			hostId, err)
	}

	// filters by hostId, which opts does not support
	vmList := make([]client.VirtualMachine, 0)
	for _, vm := range vmCollection.Data {
		if vm.HostId == hostId {
			vmList = append(vmList, vm)
		}
	}
	log.Infof("got all the vm instances with hostId %s", hostId)
	return vmList, nil
}
