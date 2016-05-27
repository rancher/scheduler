package cache

import (
	log "github.com/Sirupsen/logrus"
	rancherClient "github.com/rancher/go-rancher/client"
	schedulerClient "github.com/rancher/scheduler/client"
)

type environment struct {
	// key: host id
	HostInfos map[string]*HostInfo
}

type cacheManager struct {
	// key: environment id (account id)
	Envs map[string]*environment
}

var (
	// the only instance of cache manager
	Manager = &cacheManager{}
)

func (mgr *cacheManager) GetHostInfo(hostId string, envId string) *HostInfo {
	if mgr.Envs == nil {
		mgr.Envs = make(map[string]*environment)
	}
	env, ok := mgr.Envs[envId]
	if !ok {
		// create an environment
		env = &environment{}
		mgr.Envs[envId] = env
	}
	if env.HostInfos == nil {
		env.HostInfos = make(map[string]*HostInfo)
	}
	hostInfo, ok := env.HostInfos[hostId]
	if !ok {
		// get the host from cattle and cache it if it is a valid id
		host, err := schedulerClient.GetHost(hostId)
		if err != nil || host == nil {
			log.Errorf("can't get host from cattle using host id: %s", hostId)
			return nil
		}

		// load host info to cache
		hostInfo = loadHostInfoToCache(host)
		env.HostInfos[hostId] = hostInfo

		// load alloated resources from instances exist on this host
		err = hostInfo.loadAllocatedContainerResource()
		if err != nil {
			log.Errorf("can't loadAllocatedContainerResource for host id: %s", hostId)
			return nil
		}
		err = hostInfo.loadAllocatedVMResource()
		if err != nil {
			log.Errorf("can't loadAllocatedVMResource for host id: %s", hostId)
			return nil
		}
	}
	return hostInfo
}

// used to remove host in case it doesnt exist anymore
func RemoveHostInfoFromCache() {
	return
}

func loadHostInfoToCache(host *rancherClient.Host) *HostInfo {
	// there are several types of resources we need to load into cache
	// cpu, memory, iops
	hostInfo := &HostInfo{HostId: host.Id, EnvId: host.AccountId}
	hostInfo.Disks = make(map[string]*DiskInfo)
	hostInfo.Instances = make(map[string]*InstanceInfo)

	// get cpu count
	info, _ := host.Info.(map[string]interface{})
	cpuInfo := info["cpuInfo"].(map[string]interface{})
	cpuCount := cpuInfo["count"].(float64)
	log.Info("cpu count: ", cpuCount)

	// get memory total in MB
	memInfo := info["memoryInfo"].(map[string]interface{})
	memTotal := memInfo["memTotal"].(float64)
	log.Info("memTotal: ", memTotal)

	// get iops, we just use the first device
	iopsInfo := info["iopsInfo"].(map[string]interface{})
	for k, v := range iopsInfo {
		iops := v.(map[string]interface{})
		readIops := iops["read"].(float64)
		writeIops := iops["write"].(float64)
		k = DefaultDiskPath // just set it to default for now
		hostInfo.Disks[k] = &DiskInfo{k, IopsInfo{ReadTotal: uint64(readIops), WriteTotal: uint64(writeIops)}}
	}

	hostInfo.CpuTotalCount = cpuCount
	hostInfo.MemTotalInMB = memTotal

	return hostInfo
}

func (mgr *cacheManager) RemoveHostInfo(hostId string, envId string) {
	if mgr.Envs == nil {
		return
	}
	env, ok := mgr.Envs[envId]
	if !ok {
		return
	}
	if env.HostInfos == nil {
		return
	}
	_, ok = env.HostInfos[hostId]
	if !ok {
		return
	}
	delete(env.HostInfos, hostId)
}
