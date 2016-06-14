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
)

type Response struct {
	rancherClient.Resource
	Schedule string `json:"schedule"`
}

// ScheduleCPUMemory is a handler for route /cpu and returns a collection of host
// ids that can be scheduled on
func ScheduleCPUMemory(w http.ResponseWriter, r *http.Request) {
	log.Info("ScheduleCPUMemory called")

	hostId := r.FormValue("hostId")
	vmId := r.FormValue("vmId")
	envId := r.FormValue("envId")
	log.Infof("received hostid: %s, vmId:%s, envId: %s", hostId, vmId, envId)

	// response no means can't accomodate scheduling resource, all other cases,
	// we just say yes due to all conditions
	respNo := Response{rancherClient.Resource{Type: "schedule"}, "no"}
	resp := Response{rancherClient.Resource{Type: "schedule"}, "yes"}

	// get the hostInfo obj from cache, if not exists, it will reload
	hostInfo := cache.Manager.GetHostInfo(hostId, envId)
	if hostInfo == nil {
		log.Info("can't get host id:", hostId)
		api.GetApiContext(r).Write(&resp)
		return
	}

	// get the VM by id
	vm, err := client.GetVM(vmId)
	if err != nil || vm == nil {
		api.GetApiContext(r).Write(&resp)
		return
	}
	log.Infof("vm name: %s, vm instance id: %s", vm.Name, vm.Id)

	// calculate back from vcpu to cpu
	cpuRequired := float64(vm.Vcpu) / cache.DivisionFactorOfVcpu
	log.Infof("cpuRequired: %f", cpuRequired)

	// calculate the free resource
	freeCpu := hostInfo.CpuTotalCount - hostInfo.CpuUsed
	if freeCpu < cpuRequired {
		log.Infof("not enough cpu. require: %f, available: %f", cpuRequired, freeCpu)
		api.GetApiContext(r).Write(&respNo)
		return
	}
	log.Infof("Enough cpu. require: %f, available: %f", cpuRequired, freeCpu)
	log.Info("scheduler could accomodate cpu for vm")

	memRequiredMB := float64(vm.MemoryMb)
	log.Infof("required memRequiredMB: %f", memRequiredMB)

	// calculate the free resource
	freeMem := hostInfo.MemTotalInMB - hostInfo.MemUsedInMB
	if freeMem < memRequiredMB {
		log.Infof("not enough memory. require: %f, available: %f", memRequiredMB, freeMem)
		api.GetApiContext(r).Write(&respNo)
		return
	}
	log.Infof("Enough memory. require: %f, available: %f", memRequiredMB, freeMem)
	log.Info("scheduler could accomodate memory for vm")

	api.GetApiContext(r).Write(&resp)
}

// AllocateCPUMemory is a handler for route /cpu and returns a collection of host ids that
// can be scheduled on
func AllocateCPUMemory(w http.ResponseWriter, r *http.Request) {
	log.Info("AllocateCPUMemory called")

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
	hostInfo.AllocateCPUMemoryForVM(vm)

	return
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
func ScheduleIops(w http.ResponseWriter, r *http.Request) {
	log.Info("ScheduleIops called")

	hostId := r.FormValue("hostId")
	instanceId := r.FormValue("instanceId")
	envId := r.FormValue("envId")
	log.Infof("received hostid: %s, instanceId:%s, envId: %s", hostId, instanceId, envId)

	// response no means can't accomodate scheduling resource, all other cases,
	// we just say yes due to all conditions
	respNo := Response{rancherClient.Resource{Type: "schedule"}, "no"}
	resp := Response{rancherClient.Resource{Type: "schedule"}, "yes"}

	// get the hostInfo obj from cache, if not exists, it will reload
	hostInfo := cache.Manager.GetHostInfo(hostId, envId)
	if hostInfo == nil {
		api.GetApiContext(r).Write(&resp)
		return
	}

	var labels map[string]interface{}

	// schedule iops for both container and vm
	container, err := client.GetContainer(instanceId)
	if err != nil {
		api.GetApiContext(r).Write(&resp)
		return
	}
	if container != nil {
		labels = container.Labels
		log.Info("schedule container for iops")
	} else {
		vm, err := client.GetVM(instanceId)
		if err != nil {
			api.GetApiContext(r).Write(&resp)
			return
		}
		if vm != nil {
			labels = vm.Labels
			log.Info("schedule VM for iops")
		}
	}

	// get resource requirements
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
		api.GetApiContext(r).Write(&resp)
		return
	}
	log.Infof("readIopsRequired: %d, writeIopsRequired: %d", readIopsRequired, writeIopsRequired)

	// calculate the free resource
	if hasReadIopsLabel {
		diskInfo, ok := hostInfo.Disks[cache.DefaultDiskPath]
		if !ok {
			log.Info("no disk on host for disk path:", cache.DefaultDiskPath)
			api.GetApiContext(r).Write(&respNo)
			return
		}
		freeReadIops := diskInfo.Iops.ReadTotal - diskInfo.Iops.ReadAllocated
		if freeReadIops < readIopsRequired {
			log.Infof("not enough read iops. require: %d, available: %d", readIopsRequired, freeReadIops)
			api.GetApiContext(r).Write(&respNo)
			return
		}
		log.Infof("Enough read iops. require: %d, available: %d", readIopsRequired, freeReadIops)
	}
	if hasWriteIopsLabel {
		diskInfo, ok := hostInfo.Disks[cache.DefaultDiskPath]
		if !ok {
			log.Info("no disk on host for disk path:", cache.DefaultDiskPath)
			api.GetApiContext(r).Write(&respNo)
			return
		}
		freeWriteIops := diskInfo.Iops.WriteTotal - diskInfo.Iops.WriteAllocated
		if freeWriteIops < writeIopsRequired {
			log.Infof("not enough write iops. require: %d, available: %d", writeIopsRequired, freeWriteIops)
			api.GetApiContext(r).Write(&respNo)
			return
		}
		log.Infof("Enough write iops. require: %d, available: %d", writeIopsRequired, freeWriteIops)
	}

	log.Info("scheduler could accomodate iops for instance")

	api.GetApiContext(r).Write(&resp)
}

//ScheduleCPU is a handler for route /cpu and returns a collection of host ids that can be scheduled on
func AllocateIops(w http.ResponseWriter, r *http.Request) {
	log.Info("AllocateIops called")

	hostId := r.FormValue("hostId")
	instanceId := r.FormValue("instanceId")
	envId := r.FormValue("envId")
	log.Infof("received hostid: %s, instanceId:%s, envId: %s", hostId, instanceId, envId)

	// get the hostInfo obj from cache, if not exists, it will reload
	hostInfo := cache.Manager.GetHostInfo(hostId, envId)
	if hostInfo == nil {
		return
	}

	// allocate iops for both container and vm
	container, err := client.GetContainer(instanceId)
	if err != nil {
		return
	}
	if container != nil {
		log.Info("allocate container for iops")
		hostInfo.AllocateIopsForInstance(container.Labels, instanceId)
		return
	}
	vm, err := client.GetVM(instanceId)
	if err != nil {
		return
	}
	if vm != nil {
		log.Info("allocate VM for iops")
		hostInfo.AllocateIopsForInstance(vm.Labels, instanceId)
		return
	}
}

//ScheduleCPU is a handler for route /cpu and returns a collection of host ids that can be scheduled on
func DeallocateIops(w http.ResponseWriter, r *http.Request) {
	log.Info("DeallocateIops called")

	hostId := r.FormValue("hostId")
	instanceId := r.FormValue("instanceId")
	envId := r.FormValue("envId")
	log.Infof("received hostid: %s, instanceId:%s, envId: %s", hostId, instanceId, envId)

	// get the hostInfo obj from cache, if not exists, it will reload
	hostInfo := cache.Manager.GetHostInfo(hostId, envId)
	if hostInfo == nil {
		return
	}

	log.Infof("instance id: %s", instanceId)
	hostInfo.DeallocateIopsForInstance(instanceId)

	return
}

//ScheduleCPU is a handler for route /cpu and returns a collection of host ids that can be scheduled on
func RemoveInstance(w http.ResponseWriter, r *http.Request) {
	log.Info("RemoveInstance called")

	hostId := r.FormValue("hostId")
	instanceId := r.FormValue("instanceId")
	envId := r.FormValue("envId")
	log.Infof("received hostid: %s, instanceId:%s, envId: %s", hostId, instanceId, envId)

	// get the hostInfo obj from cache, if not exists, it will reload
	hostInfo := cache.Manager.GetHostInfo(hostId, envId)
	if hostInfo == nil {
		return
	}
	hostInfo.RemoveInstanceInfo(instanceId)
	log.Infof("removed instanceId:%s, from hostid: %s, envId: %s", instanceId, hostId, envId)
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
