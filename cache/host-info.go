package cache

import (
	log "github.com/Sirupsen/logrus"
	rancherClient "github.com/rancher/go-rancher/client"
	schedulerClient "github.com/rancher/scheduler/client"
	"strconv"
	"strings"
	"fmt"
)

type IopsInfo struct {
	ReadTotal      uint64
	ReadAllocated  uint64
	WriteTotal     uint64
	WriteAllocated uint64
}

type DiskInfo struct {
	DevicePath string
	Iops       IopsInfo
}

type HostInfo struct {
	HostId            string
	EnvId             string
	CpuTotalCount     float64
	CpuUsed           float64
	MemTotalInMB      float64
	MemUsedInMB       float64
	NotCompleteLoaded bool

	// key: device path (/dev/sda) without /dev prefix
	Disks map[string]*DiskInfo

	// key: instance id
	Instances map[string]*InstanceInfo
}

const (
	DefaultDiskPath      = "default"
	DivisionFactorOfVcpu = 2
)

// return true means allocated, otherwise not allocated anything
func (host *HostInfo) AllocateIopsForInstance(labels map[string]interface{}, instanceId string) error {
	var readIopsReserved uint64
	var writeIopsReserved uint64
	hasLabels := false
	for k, v := range labels {
		if v == nil {
			continue
		}
		labelValue := v.(string)
		if strings.HasPrefix(k, ReadIopsLabel) {
			readIopsReserved, _ = strconv.ParseUint(labelValue, 10, 64)
			hasLabels = true
		} else if strings.HasPrefix(k, WriteIopsLabel) {
			writeIopsReserved, _ = strconv.ParseUint(labelValue, 10, 64)
			hasLabels = true
		}
	}
	// no iops labels, we are done
	if hasLabels == false {
		log.Info("no iops labels")
		return nil
	}

	log.Infof("allocate iops for instance id: %s, on host id: %s, readIopsReserved: %d, writeIopsReserved: %d",
		instanceId, host.HostId, readIopsReserved, writeIopsReserved)

	// account for resource used by this instance. disk has to exist during GetHostInfo() call
	diskInfo, ok := host.Disks[DefaultDiskPath]
	if !ok {
		errStr := fmt.Sprintf("no disk on host with disk path:", DefaultDiskPath)
		log.Error(errStr)
		return fmt.Errorf(errStr)
	}
	if diskInfo.Iops.ReadAllocated + readIopsReserved > diskInfo.Iops.ReadTotal {
		errStr := fmt.Sprintf("already allocated(%d) + reserving(%d) > total(%d) iops", diskInfo.Iops.ReadAllocated, readIopsReserved, diskInfo.Iops.ReadTotal)
		log.Error(errStr)
		return fmt.Errorf(errStr)
	}
	diskInfo.Iops.ReadAllocated += readIopsReserved
	diskInfo.Iops.WriteAllocated += writeIopsReserved
	log.Infof("host id:%s, after allocation, ReadAllocated: %d, WriteAllocated: %d", host.HostId,
		diskInfo.Iops.ReadAllocated, diskInfo.Iops.WriteAllocated)

	// now cache reserved info for deallocation
	instanceInfo, ok := host.Instances[instanceId]
	if !ok {
		// create an InstanceInfo for deallocation when instance is removed
		instanceInfo = &InstanceInfo{instanceId, 0, 0, make(map[string]*DiskReserved)}
		host.Instances[instanceId] = instanceInfo
	}

	// create and add an entry (replace it if exists)
	diskReserved := &DiskReserved{DefaultDiskPath, readIopsReserved, writeIopsReserved}
	instanceInfo.DisksReservedMap[DefaultDiskPath] = diskReserved

	return nil
}

// return true means allocated, otherwise not allocated anything
func (host *HostInfo) DeallocateIopsForInstance(instanceId string) (err error) {
	err = nil
	instanceInfo, ok := host.Instances[instanceId]
	if !ok {
		log.Info("never allocated for instance id: ", instanceId)
		return
	}
	diskReserved, ok := instanceInfo.DisksReservedMap[DefaultDiskPath]
	if !ok {
		log.Info("no disk reserved for instance id: ", instanceId, " with disk path", DefaultDiskPath)
		return
	}
	readIopsReserved := diskReserved.ReadIopsReserved
	writeIopsReserved := diskReserved.WriteIopsReserved
	log.Infof("deallocate iops for instance id: %s, on host id: %s, readIopsReserved: %d, writeIopsReserved: %d",
		instanceId, host.HostId, readIopsReserved, writeIopsReserved)

	// account for resource used by this instance
	diskInfo, ok := host.Disks[DefaultDiskPath]
	if !ok {
		log.Infof("no allocation of iops from device path: %s", DefaultDiskPath)
		return
	}
	if diskInfo.Iops.ReadAllocated < readIopsReserved {
		errStr := fmt.Sprintf("deallocate (%d) more iops than allocated (%d), it should never happen", readIopsReserved, diskInfo.Iops.ReadAllocated)
		log.Error(errStr)
		return fmt.Errorf(errStr)
	}
	diskInfo.Iops.ReadAllocated -= readIopsReserved
	diskInfo.Iops.WriteAllocated -= writeIopsReserved
	log.Infof("host id:%s, after deallocation, ReadAllocated: %d, WriteAllocated: %d", host.HostId,
		diskInfo.Iops.ReadAllocated, diskInfo.Iops.WriteAllocated)

	// remove a disksReserved map entry for instance
	delete(instanceInfo.DisksReservedMap, DefaultDiskPath)
	return
}

// instance resources are iops
func (host *HostInfo) loadAllocatedContainerResource() error {
	log.Infof("Loading all allocated container resouces on the host id: %s ...", host.HostId)

	containerList, err := schedulerClient.GetContainersOnHost(host.HostId, host.EnvId)
	if err != nil || containerList == nil || len(containerList) == 0 {
		log.Infof("No containers on the host id: %s", host.HostId)
		return err
	}
	for _, container := range containerList {
		log.Infof("container name: %s, container id: %s", container.Name, container.Id)

		// allocate resource for this container. container now just uses iops, no cpu/mem
		host.AllocateIopsForInstance(container.Labels, container.Id)
	}
	log.Infof("Done loading all allocated container resouces on the host id: %s", host.HostId)

	return nil
}

// VM resources are cpu/mem
func (host *HostInfo) loadAllocatedVMResource() error {
	log.Infof("Loading all allocated VM resouces on the host id: %s ...", host.HostId)

	// first time we need to get all the container instances from cattle
	// scheduled on that host
	vmList, err := schedulerClient.GetVMsOnHost(host.HostId, host.EnvId)
	if err != nil || vmList == nil || len(vmList) == 0 {
		log.Infof("No VMs on the host id: %s", host.HostId)
		return err
	}
	for _, vm := range vmList {
		log.Infof("vm name: %s, vm id: %s", vm.Name, vm.Id)

		// vm need cpu and memory
		host.AllocateCPUMemoryForVM(&vm)

		// vm could reserve iops
		host.AllocateIopsForInstance(vm.Labels, vm.Id)
	}
	log.Infof("Done loading all allocated VM resouces on the host id: %s", host.HostId)

	return nil
}

func (host *HostInfo) AllocateCPUMemoryForVM(vm *rancherClient.VirtualMachine) error {
	// calculate back from vcpu to cpu
	cpuReserved := float64(vm.Vcpu) / DivisionFactorOfVcpu
	memReserved := float64(vm.MemoryMb)
	log.Infof("cpuReserved: %f, memReserved: %f", cpuReserved, memReserved)
	log.Infof("allocate cpu and memory for vm id: %s, on host id: %s, cpuReserved: %f, memReserved: %f",
		vm.Id, host.HostId, cpuReserved, memReserved)

	// account for cpu/mem resource used by this instance
	if host.CpuUsed + cpuReserved > host.CpuTotalCount {
		errStr := fmt.Sprintf("already used(%f) + reserving(%f) > CpuTotalCount(%f)", host.CpuUsed, cpuReserved, host.CpuTotalCount)
		log.Error(errStr)
		return fmt.Errorf(errStr)
	}
	host.CpuUsed += cpuReserved
	host.MemUsedInMB += memReserved
	log.Infof("host id: %s, after allocation, CpuUsed: %f, memReserved: %f", host.HostId,
		host.CpuUsed, host.MemUsedInMB)

	// update instanceInfo if exists or create a new one
	instanceInfo, ok := host.Instances[vm.Id]
	if !ok {
		// create an InstanceInfo for deallocation when instance is removed
		host.Instances[vm.Id] = &InstanceInfo{vm.Id, cpuReserved, memReserved, make(map[string]*DiskReserved)}
	} else {
		instanceInfo.CpuReserved = cpuReserved
		instanceInfo.MemReservedInMB = memReserved
	}

	return nil
}

func (host *HostInfo) DeallocateCPUMemoryForVM(instanceId string) (err error) {
	err = nil
	instanceInfo, ok := host.Instances[instanceId]
	if !ok {
		return
	}
	cpuReserved := instanceInfo.CpuReserved
	memReserved := instanceInfo.MemReservedInMB
	log.Infof("deallocate cpu and memory for instance id: %s, on host id: %s, cpuReserved: %f, memReserved: %f",
		instanceId, host.HostId, cpuReserved, memReserved)

	if host.CpuUsed < cpuReserved {
		errStr := fmt.Sprintf("deallocate (%f) more cpu than allocated (%f), it should never happen", cpuReserved, host.CpuUsed)
		log.Error(errStr)
		return fmt.Errorf(errStr)
	}
	host.CpuUsed -= cpuReserved
	host.MemUsedInMB -= memReserved
	log.Infof("host id:%s, after deallocation, CpuUsed: %f, MemUsedInMB: %f", host.HostId,
		host.CpuUsed, host.MemUsedInMB)

	return
}

func (host *HostInfo) RemoveInstanceInfo(instanceId string) {
	_, ok := host.Instances[instanceId]
	if !ok {
		return
	}
	delete(host.Instances, instanceId)
}
