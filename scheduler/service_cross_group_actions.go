package scheduler

import (
	"fmt"
	"github.com/Sirupsen/logrus"
	"github.com/rancher/go-rancher-metadata/metadata"
	"strconv"
	"strings"
	"time"
)

const (
	serviceLabel     = "io.rancher.stack_service.name"
	zoneSplitsLabels = "io.rancher.scheduler.scale_per_group"
)

type ScaleGroupFilter struct {
}

func (s ScaleGroupFilter) Filter(scheduler *Scheduler, resourceRequest []ResourceRequest, context Context, hosts []string) []string {
	if context == nil || len(context) == 0 || context[0].Data.Fields.Labels == nil {
		return hosts
	}
	if _, ok := context[0].Data.Fields.Labels[zoneSplitsLabels]; ok {
		if serviceName, ok := context[0].Data.Fields.Labels[serviceLabel]; ok {
			// service is not updated in metadata yet, update service manually
			service := metadata.Service{}
			if parts := strings.SplitN(serviceName, "/", 2); len(parts) == 2 {
				service.StackName = parts[0]
				service.Name = parts[1]
				service.Labels = map[string]string{}
				service.Labels[zoneSplitsLabels] = context[0].Data.Fields.Labels[zoneSplitsLabels]
				scheduler.UpdateService([]metadata.Service{service})
			}
			if md, ok := scheduler.services[serviceName]; ok {
				return runDistributionAlgorithm(scheduler, md, context, hosts)
			}
		}
	}

	return hosts
}

// ScaleGroupReserveAction is a reserve action for service scale group
type ScaleGroupReserveAction struct {
}

func (s *ScaleGroupReserveAction) Reserve(scheduler *Scheduler, requests []ResourceRequest, context Context, host *host, force bool, data map[string]interface{}) error {
	for _, con := range context {
		if serviceName, ok := con.Data.Fields.Labels[serviceLabel]; ok {
			if serviceMd, ok := scheduler.services[serviceName]; ok {
				if hostLabels, ok := host.pools[hostLabels].(*LabelPool); ok {
					if zone, ok := hostLabels.Labels[serviceMd.key]; ok {
						serviceMd.currentDistribution[strings.ToLower(zone)]++
						serviceMd.total++
						if len(context) > 0 {
							if b, ok := scheduler.signals[context[0].DeploymentUnitUUID]; ok && b {
								serviceMd.currentDistribution[strings.ToLower(zone)]--
								serviceMd.total--
							}
						}
					}
				}
			}
		}
	}
	if len(context) > 0 {
		scheduler.signals[context[0].DeploymentUnitUUID] = false
	}
	return nil
}

func (s *ScaleGroupReserveAction) RollBack(scheduler *Scheduler, requests []ResourceRequest, context Context, host *host) {
	return
}

type ScaleGroupReleaseAction struct {
}

func (s ScaleGroupReleaseAction) Release(scheduler *Scheduler, requests []ResourceRequest, context Context, host *host) {
	for _, con := range context {
		if _, ok := con.Data.Fields.Labels[serviceLabel]; !ok {
			return
		}
		serviceName := con.Data.Fields.Labels[serviceLabel]
		if serviceMd, ok := scheduler.services[serviceName]; ok {
			if hostLabels, ok := host.pools[hostLabels].(*LabelPool); ok {
				if zone, ok := hostLabels.Labels[serviceMd.key]; ok {
					if serviceMd.currentDistribution[zone] > 0 {
						serviceMd.currentDistribution[zone]--
					}
					if serviceMd.total > 0 {
						serviceMd.total--
					}
				}
			}
		}
	}
}

func constructServiceWithLabel(s *Scheduler, service metadata.Service) {
	if _, ok := service.Labels[zoneSplitsLabels]; !ok {
		return
	}
	val := service.Labels[zoneSplitsLabels]
	logrus.Infof("updating service %s into scheduler. Service Weight label: %s", service.Name, val)
	if data, ok := s.services[fmt.Sprintf("%s/%s", service.StackName, service.Name)]; !ok {
		parts := strings.Split(strings.TrimSpace(val), ";")
		if len(parts) == 1 || parts[1] == "" {
			// format like this: io.rancher.scheduler.scale_per_group: key=zone, evenly distributed
			// collecting zones across all the existing hosts
			result := map[string]int64{}
			curmap := map[string]int64{}
			key := strings.Split(parts[0], "=")[1]
			for _, host := range s.hosts {
				hostLabels := host.pools[hostLabels].(*LabelPool).Labels
				if v, ok := hostLabels[key]; ok {
					result[v] = 1
					curmap[v] = 0
				}
			}
			totalWeights := len(result)
			md := &serviceMetadata{
				desiredDistribution: result,
				currentDistribution: curmap,
				totalWeight:         int64(totalWeights),
				key:                 strings.ToLower(strings.TrimSpace(key)),
			}
			s.services[fmt.Sprintf("%s/%s", service.StackName, service.Name)] = md
			populateContainers(service, md, s)
		} else if len(parts) == 2 {
			// format like this: io.rancher.scheduler.scale_per_group: key=zone; west-1=1, west-2=2
			result := map[string]int64{}
			curmap := map[string]int64{}
			key := strings.Split(parts[0], "=")[1]
			totalWeights := 0
			values := strings.Split(parts[1], ",")
			for _, vs := range values {
				vs = strings.ToLower(strings.TrimSpace(vs))
				if vs != "" && len(strings.Split(vs, "=")) == 2 {
					k := strings.TrimSpace(strings.Split(vs, "=")[0])
					v, _ := strconv.Atoi(strings.Split(vs, "=")[1])
					if v != 0 {
						result[k] = int64(v)
						curmap[k] = 0
						totalWeights += v
					}
				}
			}
			md := &serviceMetadata{
				desiredDistribution: result,
				currentDistribution: curmap,
				totalWeight:         int64(totalWeights),
				key:                 strings.ToLower(strings.TrimSpace(key)),
			}
			s.services[fmt.Sprintf("%s/%s", service.StackName, service.Name)] = md
			populateContainers(service, md, s)
		}
	} else {
		// if service exists in the map, only update weights
		parts := strings.Split(strings.TrimSpace(val), ";")
		if len(parts) == 1 || parts[1] == "" {
			result := map[string]int64{}
			key := strings.Split(parts[0], "=")[1]
			for _, host := range s.hosts {
				hostLabels := host.pools[hostLabels].(*LabelPool).Labels
				if v, ok := hostLabels[key]; ok {
					result[v] = 1
				}
			}
			totalWeights := len(result)
			data.desiredDistribution = result
			data.key = strings.ToLower(strings.TrimSpace(key))
			data.totalWeight = int64(totalWeights)
		} else if len(parts) == 2 {
			result := map[string]int64{}
			key := strings.Split(parts[0], "=")[1]
			totalWeights := 0
			values := strings.Split(parts[1], ",")
			for _, vs := range values {
				vs = strings.ToLower(strings.TrimSpace(vs))
				if vs != "" && len(strings.Split(vs, "=")) == 2 {
					k := strings.TrimSpace(strings.Split(vs, "=")[0])
					v, _ := strconv.Atoi(strings.Split(vs, "=")[1])
					if v != 0 {
						result[k] = int64(v)
						totalWeights += v
					}
				}
			}
			data.desiredDistribution = result
			data.key = strings.ToLower(strings.TrimSpace(key))
			data.totalWeight = int64(totalWeights)
		}
	}
}

//runDistributionAlgorithm is a mystical method to distribute instances across different zones given weights
func runDistributionAlgorithm(scheduler *Scheduler, md *serviceMetadata, context Context, hosts []string) []string {
	realMap := md.desiredDistribution
	curmap := md.currentDistribution
	selectedZone := ""
	// first, find if any zone is empty. if so, fill that first
	for zone, count := range curmap {
		if hs := getHostsFromZone(zone, md, scheduler, hosts, false); len(hs) > 0 {
			if count == 0 {
				logrus.Infof("Current Zone Distribution %v. Selected Zone %v", md.currentDistribution, selectedZone)
				increaseTempVal(scheduler, md, zone, context)
				selectedZone = zone
				return getHostsFromZone(zone, md, scheduler, hosts, true)
			}
		}
	}
	// calculated based on percentage
	percentageDiff := map[string]float64{}
	max := -100.0
	realTotalWeight := md.totalWeight
	for k, val := range realMap {
		if hs := getHostsFromZone(k, md, scheduler, hosts, false); len(hs) == 0 {
			realTotalWeight -= val
		}
	}
	for k, val := range realMap {
		if hs := getHostsFromZone(k, md, scheduler, hosts, false); len(hs) > 0 {
			percentageDiff[k] = float64(val)/float64(realTotalWeight) - float64(curmap[k])/float64(md.total)
			if percentageDiff[k] > max {
				selectedZone = k
				max = percentageDiff[k]
			}
		}
	}
	if selectedZone != "" {
		logrus.Infof("Current Zone Distribution %v. Selected Zone %v", md.currentDistribution, selectedZone)
		increaseTempVal(scheduler, md, selectedZone, context)
		return getHostsFromZone(selectedZone, md, scheduler, hosts, true)
	}
	return []string{}
}

// getHostsFromZone return selected hosts and not select hosts, selected hosts are ordered first
func getHostsFromZone(selectedZone string, md *serviceMetadata, scheduler *Scheduler, hosts []string, soft bool) []string {
	finalCandidates := []string{}
	notSelected := []string{}
	for _, hostUUID := range hosts {
		host := scheduler.hosts[hostUUID]
		labels := host.pools[hostLabels].(*LabelPool).Labels
		if strings.EqualFold(strings.TrimSpace(labels[md.key]), strings.TrimSpace(selectedZone)) {
			finalCandidates = append(finalCandidates, hostUUID)
		} else if _, ok := labels[md.key]; ok {
			notSelected = append(notSelected, hostUUID)
		}
	}
	if soft {
		return append(finalCandidates, notSelected...)
	}
	return finalCandidates
}

func populateContainers(service metadata.Service, md *serviceMetadata, s *Scheduler) {
	containers := service.Containers
	for _, con := range containers {
		hostUUID := con.HostUUID
		if host, ok := s.hosts[hostUUID]; ok {
			labels := host.pools[hostLabels].(*LabelPool).Labels
			if val, ok := labels[md.key]; ok {
				md.currentDistribution[val]++
				md.total++
			}
		}
	}
}

//increaseTempVal is a method to do a fake reserve in prioritize events to prevent race condition
func increaseTempVal(scheduler *Scheduler, md *serviceMetadata, zone string, context Context) {
	if scheduler.sleepTime >= 0 {
		md.currentDistribution[zone] += int64(len(context))
		md.total += int64(len(context))
		if len(context) > 0 {
			scheduler.signals[context[0].DeploymentUnitUUID] = true
		}
		go func() {
			time.Sleep(time.Second * time.Duration(scheduler.sleepTime))
			scheduler.mu.Lock()
			scheduler.signals[context[0].DeploymentUnitUUID] = false
			scheduler.mu.Unlock()
		}()
	}
}
