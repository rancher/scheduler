package service

import (
	log "github.com/Sirupsen/logrus"
	"github.com/rancher/go-rancher/api"
	rancherClient "github.com/rancher/go-rancher/client"
	"github.com/rancher/scheduler/cache"
	"github.com/rancher/scheduler/client"
	"net/http"
	"strconv"
	"strings"
	"fmt"
)

type ScheduleResponse struct {
	// have to include below anonymous resource field
	rancherClient.Resource
	Schedule []string `json:"schedule"`
	ErrorMessage string `json:"errorMessage"`
}

type ModifyResponse struct {
	// have to include below anonymous resource field
	rancherClient.Resource
	ErrorMessage string `json:"errorMessage"`
}

const (
	INSTANCE_KIND_VIRTUAL_MACHINE = "virtualMachine"
	INSTANCE_KIND_CONTAINER = "container"
)

// ScheduleCPUMemory is a handler for route /cpu and returns a collection of host
// ids that can be scheduled on
func Schedule(w http.ResponseWriter, r *http.Request) {
	r.ParseForm()
	hostIds := r.Form["hostIds"]
	instanceId := r.FormValue("instanceId")
	instanceKind := r.FormValue("instanceKind")
	envId := r.FormValue("envId")

	log.Infof("Schedule for %s, instanceId: %s, envId: %s, hostIds: %s", instanceKind, instanceId, envId, hostIds)

	schedulableHostIds := make([]string, 0)
	var requirement string
	var errorMessage string

	// schedule Iops for all instances, disregard its kind
	schedulableHostIds, requirement = ScheduleIops(hostIds, instanceId, instanceKind, envId)
	if schedulableHostIds == nil || len(schedulableHostIds) == 0 {
		errorMessage = fmt.Sprintf("Iops requirements: %s", requirement)
		goto done
	}
	if strings.EqualFold(instanceKind, INSTANCE_KIND_VIRTUAL_MACHINE) {
		schedulableHostIds, requirement = ScheduleCpu(hostIds, instanceId, envId)
	}
	if schedulableHostIds == nil || len(schedulableHostIds) == 0 {
		errorMessage = fmt.Sprintf("Vcpu requirement: %s", requirement)
		goto done
	}
	if strings.EqualFold(instanceKind, INSTANCE_KIND_VIRTUAL_MACHINE) {
		schedulableHostIds, requirement = ScheduleMemory(schedulableHostIds, instanceId, envId)
	}
	if schedulableHostIds == nil || len(schedulableHostIds) == 0 {
		errorMessage = fmt.Sprintf("Memory(in MB) requirement: %s", requirement)
		goto done
	}

done:

	response := ScheduleResponse{
		rancherClient.Resource{
			Type: "scheduler",
		},
		schedulableHostIds,
		errorMessage,
	}
	api.CreateApiContext(w, r, schemas)
	api.GetApiContext(r).Write(&response)
	return
}


func ScheduleCpu(hostIds []string, vmId string, envId string) (schedulableHostIds []string, requirement string) {
	log.Infof("ScheduleCpu for VM id: %s, envId: %s", vmId, envId)

	// default return value
	schedulableHostIds = make([]string, 0)

	// get the VM by id
	vm, err := client.GetVM(vmId)
	if err != nil || vm == nil {
		// can't get VM, we have to assume all hosts work
		schedulableHostIds = hostIds
		return
	}
	requirement = string(vm.Vcpu)

	for _, hostId := range hostIds {
		isSchedulable := ScheduleCpuOnHost(hostId, vm, envId)
		if isSchedulable {
			schedulableHostIds = append(schedulableHostIds, hostId)
		}
	}

	return
}

func ScheduleCpuOnHost(hostId string, vm *rancherClient.VirtualMachine, envId string) bool {
	log.Infof("ScheduleCpuOnHost: vm name: %s, vmId:%s, against hostid: %s", vm.Name, vm.Id, hostId)

	// return false means can't accomodate scheduling resource,
	// default to true for all other conditions
	retVal := true

	// get the hostInfo obj from cache, if not exists, it will reload
	hostInfo := cache.Manager.GetHostInfo(hostId, envId)
	if hostInfo == nil {
		log.Info("can't get hostInfo id:", hostId)
		return retVal
	}

	// calculate back from vcpu to cpu
	cpuRequired := float64(vm.Vcpu) / cache.DivisionFactorOfVcpu
	log.Infof("cpuRequired: %f", cpuRequired)

	// calculate the free resource
	freeCpu := hostInfo.CpuTotalCount - hostInfo.CpuUsed
	if freeCpu < cpuRequired {
		log.Infof("not enough cpu. require: %f, available: %f", cpuRequired, freeCpu)
		return false
	}
	log.Infof("Enough cpu. require: %f, available: %f", cpuRequired, freeCpu)
	log.Info("scheduler could accomodate cpu for vm")

	return retVal
}

func ScheduleMemory(hostIds []string, vmId string, envId string) (schedulableHostIds []string, requirement string) {
	log.Infof("ScheduleMemory for VM id: %s, envId: %s", vmId, envId)

	// default return value
	schedulableHostIds = make([]string, 0)

	// get the VM by id
	vm, err := client.GetVM(vmId)
	if err != nil || vm == nil {
		// can't get VM, we have to assume all hosts work
		schedulableHostIds = hostIds
		return
	}

	for _, hostId := range hostIds {
		isSchedulable := ScheduleMemoryOnHost(hostId, vm, envId)
		if isSchedulable {
			schedulableHostIds = append(schedulableHostIds, hostId)
		}
	}

	return
}

func ScheduleMemoryOnHost(hostId string, vm *rancherClient.VirtualMachine, envId string) bool {
	log.Infof("ScheduleMemoryOnHost: vm name: %s, vmId:%s, against hostid: %s", vm.Name, vm.Id, hostId)

	// return false means can't accomodate scheduling resource,
	// default to true for all other conditions
	retVal := true

	// get the hostInfo obj from cache, if not exists, it will reload
	hostInfo := cache.Manager.GetHostInfo(hostId, envId)
	if hostInfo == nil {
		log.Info("can't get hostInfo id:", hostId)
		return retVal
	}

	memRequiredMB := float64(vm.MemoryMb)
	log.Infof("required memRequiredMB: %f", memRequiredMB)

	// calculate the free resource
	freeMem := hostInfo.MemTotalInMB - hostInfo.MemUsedInMB
	if freeMem < memRequiredMB {
		log.Infof("not enough memory. require: %f, available: %f", memRequiredMB, freeMem)
		return false
	}
	log.Infof("Enough memory. require: %f, available: %f", memRequiredMB, freeMem)
	log.Info("scheduler could accomodate memory for vm")

	return retVal
}

func AllocateCpuMemory(hostId string, vmId string, envId string) error {
	log.Infof("AllocateCpuMemory hostid: %s, vmId:%s, envId: %s", hostId, vmId, envId)

	// get the hostInfo obj from cache, if not exists, it will reload
	hostInfo := cache.Manager.GetHostInfo(hostId, envId)
	if hostInfo == nil {
		return fmt.Errorf("can't get hostInfo id: %s", hostId)
	}

	// get the VM by id
	vm, err := client.GetVM(vmId)
	if err != nil || vm == nil {
		return fmt.Errorf("can't get VM id: %s", vmId)
	}

	err = hostInfo.AllocateCPUMemoryForVM(vm)

	return err
}

// DeallocateCPUMemory is a handler for route /cpu and returns a collection of host ids that can
// be scheduled on
func DeallocateCPUMemory(w http.ResponseWriter, r *http.Request) {
	log.Info("DeallocateCPUMemory called")

	hostId := r.FormValue("hostId")
	vmId := r.FormValue("vmId")
	envId := r.FormValue("envId")
	log.Infof("received hostid: %s, vmId:%s, envId: %s", hostId, vmId, envId)

	// get the hostInfo obj from cache, if not exists, it will reload
	hostInfo := cache.Manager.GetHostInfo(hostId, envId)
	if hostInfo == nil {
		return
	}

	// get the VM by id
	vm, err := client.GetVM(vmId)
	if err != nil || vm == nil {
		return
	}
	log.Infof("vm name: %s, vm instance id: %s", vm.Name, vm.Id)
	hostInfo.DeallocateCPUMemoryForVM(vm.Id)

	return
}


//ScheduleCPU is a handler for route /cpu and returns a collection of host ids that can be scheduled on
func ScheduleIops(hostIds []string, instanceId string, instanceKind string, envId string) (schedulableHostIds []string, requirement string) {
	log.Infof("ScheduleIops for %s id: %s, envId: %s", instanceKind, instanceId, envId)

	// default return value
	schedulableHostIds = make([]string, 0)

	var labels map[string]interface{}

	// schedule iops for both container or vm
	if strings.EqualFold(instanceKind, INSTANCE_KIND_CONTAINER) {
		container, err := client.GetContainer(instanceId)
		if err != nil {
			// can't get container, we have to assume all hosts work
			schedulableHostIds = hostIds
			return
		}
		labels = container.Labels
	} else {
		vm, err := client.GetVM(instanceId)
		if err != nil {
			// can't get vm, we have to assume all hosts work
			schedulableHostIds = hostIds
			return
		}
		labels = vm.Labels
	}

	// get resource requirements, it will be a map in multiple disks scenario
	var readIopsRequired uint64
	var writeIopsRequired uint64
	hasReadIopsLabel := false
	hasWriteIopsLabel := false
	for k, v := range labels {
		if v == nil {
			continue
		}
		labelValue := v.(string)
		if strings.HasPrefix(k, cache.ReadIopsLabel) {
			readIopsRequired, _ = strconv.ParseUint(labelValue, 10, 64)
			hasReadIopsLabel = true
		} else if strings.HasPrefix(k, cache.WriteIopsLabel) {
			writeIopsRequired, _ = strconv.ParseUint(labelValue, 10, 64)
			hasWriteIopsLabel = true
		}
	}
	if !hasReadIopsLabel && !hasWriteIopsLabel {
		log.Info("no iops labels")

		// we have to assume all hosts work
		schedulableHostIds = hostIds
		return
	}
	requirement = fmt.Sprintf("readIopsRequired: %d, writeIopsRequired: %d", readIopsRequired, writeIopsRequired)
	log.Info(requirement)

	for _, hostId := range hostIds {
		isSchedulable := ScheduleIopsOnHost(hostId, envId, readIopsRequired, writeIopsRequired)
		if isSchedulable {
			schedulableHostIds = append(schedulableHostIds, hostId)
		}
	}

	return
}


//ScheduleCPU is a handler for route /cpu and returns a collection of host ids that can be scheduled on
func ScheduleIopsOnHost(hostId string, envId string, readIopsRequired uint64, writeIopsRequired uint64) bool {
	log.Infof("ScheduleIopsOnHost hostid: %s, envId: %s, Iops requirements: readIopsRequired: %d, writeIopsRequired: %d",
			hostId, envId, readIopsRequired, writeIopsRequired)

	// return false means can't accomodate scheduling resource,
	// default to true for all other conditions
	retVal := true

	// get the hostInfo obj from cache, if not exists, it will reload
	hostInfo := cache.Manager.GetHostInfo(hostId, envId)
	if hostInfo == nil {
		return retVal
	}

	// calculate the free resource
	if readIopsRequired != 0 {
		diskInfo, ok := hostInfo.Disks[cache.DefaultDiskPath]
		if !ok {
			log.Info("no disk on host for disk path:", cache.DefaultDiskPath)
			return false
		}
		freeReadIops := diskInfo.Iops.ReadTotal - diskInfo.Iops.ReadAllocated
		if freeReadIops < readIopsRequired {
			log.Infof("not enough read iops. require: %d, available: %d", readIopsRequired, freeReadIops)
			return false
		}
		log.Infof("Enough read iops. require: %d, available: %d", readIopsRequired, freeReadIops)
	}
	if writeIopsRequired != 0 {
		diskInfo, ok := hostInfo.Disks[cache.DefaultDiskPath]
		if !ok {
			log.Info("no disk on host for disk path:", cache.DefaultDiskPath)
			return false
		}
		freeWriteIops := diskInfo.Iops.WriteTotal - diskInfo.Iops.WriteAllocated
		if freeWriteIops < writeIopsRequired {
			log.Infof("not enough write iops. require: %d, available: %d", writeIopsRequired, freeWriteIops)
			return false
		}
		log.Infof("Enough write iops. require: %d, available: %d", writeIopsRequired, freeWriteIops)
	}

	log.Info("scheduler could accomodate iops for instance")

	return retVal
}

func AllocateIops(hostId string, instanceId string, instanceKind string, envId string) error {
	log.Infof("AllocateIops for %s, instanceId: %s, on hostid: %s, envId: %s", instanceKind, instanceId, hostId, envId)

	// get the hostInfo obj from cache, if not exists, it will reload
	hostInfo := cache.Manager.GetHostInfo(hostId, envId)
	if hostInfo == nil {
		return fmt.Errorf("can't get hostInfo id: %s", hostId)
	}
	var labels map[string]interface{}

	// allocate iops for both container and vm
	if strings.EqualFold(instanceKind, INSTANCE_KIND_CONTAINER) {
		container, err := client.GetContainer(instanceId)
		if err != nil || container == nil {
			return fmt.Errorf("can't get container from cattle with instanceId: %s", instanceId)
		}
		labels = container.Labels
	} else {
		vm, err := client.GetVM(instanceId)
		if err != nil || vm == nil {
			return fmt.Errorf("can't get VM from cattle with instanceId: %s", instanceId)
		}
		labels = vm.Labels
	}
	err := hostInfo.AllocateIopsForInstance(labels, instanceId)

	return err
}

//ScheduleCPU is a handler for route /cpu and returns a collection of host ids that can be scheduled on
func DeallocateIops(hostId string, instanceId string, instanceKind string, envId string) error {
	log.Infof("DeallocateIops: for %s id: %s, hostId: %s, envId: %s", instanceKind, instanceId, hostId, envId)

	// get the hostInfo obj from cache, if not exists, it will reload
	hostInfo := cache.Manager.GetHostInfo(hostId, envId)
	if hostInfo == nil {
		log.Info("can't get hostInfo id:", hostId)
		return nil
	}

	log.Infof("instance id: %s", instanceId)
	err := hostInfo.DeallocateIopsForInstance(instanceId)

	return err
}

//ScheduleCPU is a handler for route /cpu and returns a collection of host ids that can be scheduled on
func RemoveInstance(w http.ResponseWriter, r *http.Request) {
	hostId := r.FormValue("hostId")
	instanceId := r.FormValue("instanceId")
	instanceKind := r.FormValue("instanceKind")
	envId := r.FormValue("envId")

	log.Infof("RemoveInstance from hostId: %s, envId: %s, for %s instanceId: %s", hostId, envId, instanceKind, instanceId)

	var errorMessage string

	// get the hostInfo obj from cache, if not exists, it will reload
	hostInfo := cache.Manager.GetHostInfo(hostId, envId)
	if hostInfo == nil {
		log.Info("can't get hostInfo id:", hostId)
		goto done
	}
	hostInfo.RemoveInstanceInfo(instanceId)
	log.Infof("removed %s id: %s, from hostid: %s, envId: %s", instanceKind, instanceId, hostId, envId)

done:
	response := ModifyResponse{
		rancherClient.Resource{
			Type: "scheduler",
		},
		errorMessage,
	}
	api.CreateApiContext(w, r, schemas)
	api.GetApiContext(r).Write(&response)
	return

}

func RemoveHost(w http.ResponseWriter, r *http.Request) {
	log.Info("RemoveHost called")

	hostId := r.FormValue("hostId")
	envId := r.FormValue("envId")
	log.Infof("received hostid: %s, envId: %s", hostId, envId)

	// get the hostInfo obj from cache, if not exists, it will reload
	cache.Manager.RemoveHostInfo(hostId, envId)
	log.Infof("removed hostid: %s, envId: %s", hostId, envId)
	return
}

func Allocate(w http.ResponseWriter, r *http.Request) {
	hostId := r.FormValue("hostId")
	instanceId := r.FormValue("instanceId")
	instanceKind := r.FormValue("instanceKind")
	envId := r.FormValue("envId")

	log.Infof("Allocate host resource on hostId: %s, envId: %s, for %s instanceId: %s", hostId, envId, instanceKind, instanceId)

	var errorMessage string

	// schedule Iops for all instances, disregard its kind
	err := AllocateIops(hostId, instanceId, instanceKind, envId)
	if err != nil {
		errorMessage = fmt.Sprintf("AllocateIops failed on host id: %s, for %s instanceId: %s, %s", hostId, instanceKind, instanceId, err)
		goto done
	}
	if strings.EqualFold(instanceKind, INSTANCE_KIND_VIRTUAL_MACHINE) {
		err = AllocateCpuMemory(hostId, instanceId, envId)
		if err != nil {
			errorMessage = fmt.Sprintf("AllocateCpuMemory failed on host id: %s, for %s instanceId: %s, %s", hostId, instanceKind, instanceId, err)
			goto done
		}
	}

done:

	response := ModifyResponse{
		rancherClient.Resource{
			Type: "scheduler",
		},
		errorMessage,
	}
	api.CreateApiContext(w, r, schemas)
	api.GetApiContext(r).Write(&response)
	return
}

func Deallocate(w http.ResponseWriter, r *http.Request) {
	hostId := r.FormValue("hostId")
	instanceId := r.FormValue("instanceId")
	instanceKind := r.FormValue("instanceKind")
	envId := r.FormValue("envId")

	log.Infof("Deallocate host resource on hostId: %s, envId: %s, for %s instanceId: %s", hostId, envId, instanceKind, instanceId)

	var errorMessage string
	var err error

	// get the hostInfo obj from cache, if not exists, it will reload
	hostInfo := cache.Manager.GetHostInfo(hostId, envId)
	if hostInfo == nil {
		log.Info("can't get hostInfo id:", hostId)
		goto done
	}

	// deallocate Iops for all instances, disregard its kind
	log.Infof("DeallocateIops: for %s id: %s, hostId: %s, envId: %s", instanceKind, instanceId, hostId, envId)
	err = hostInfo.DeallocateIopsForInstance(instanceId)
	if err != nil {
		errorMessage = fmt.Sprintf("DeallocateIops failed on host id: %s, for %s instanceId: %s, %s", hostId, instanceKind, instanceId, err)
		goto done
	}
	if strings.EqualFold(instanceKind, INSTANCE_KIND_VIRTUAL_MACHINE) {
		vm, err := client.GetVM(instanceId)
		if err != nil || vm == nil {
			log.Info("can't get VM id:", instanceId)
			goto done
		}
		log.Infof("vm name: %s, vm instance id: %s", vm.Name, vm.Id)
		err = hostInfo.DeallocateCPUMemoryForVM(vm.Id)
		if err != nil {
			errorMessage = fmt.Sprintf("DeallocateCPUMemoryForVM failed on host id: %s, for %s instanceId: %s, %s", hostId, instanceKind, instanceId, err)
			goto done
		}
	}

done:

	response := ModifyResponse{
		rancherClient.Resource{
			Type: "scheduler",
		},
		errorMessage,
	}
	api.CreateApiContext(w, r, schemas)
	api.GetApiContext(r).Write(&response)
	return
}