package scheduler

import (
	"github.com/Sirupsen/logrus"
)

const (
	vpcSubnetLabel            = "io.rancher.vpc.subnet"
	tempDeploymentUnitPool    = "tempDeploymentUnitPool"
	currentDeploymentUnitPool = "currentDeploymentUnitPool"
)

type VpcSubnetFilter struct {
}

func (v VpcSubnetFilter) Filter(scheduler *Scheduler, resourceRequest []ResourceRequest, context Context, hosts []string) []string {
	logrus.Debugf("Filter context: %+v", context)
	var matchHost string
breakLabel:
	for _, host := range hosts {
		lpool, ok := scheduler.hosts[host].pools["hostLabels"]
		if !ok {
			continue
		}
		val, ok := lpool.(*LabelPool).Labels[vpcSubnetLabel]
		if !ok || val == "" {
			continue
		}

		tempDpool, ok := scheduler.hosts[host].pools[tempDeploymentUnitPool]
		if !ok {
			continue
		}
		curDpool, ok := scheduler.hosts[host].pools[currentDeploymentUnitPool]
		if !ok {
			continue
		}

		allDeployments := append(tempDpool.(*DeploymentUnitPool).Deployments, curDpool.(*DeploymentUnitPool).Deployments...)
		logrus.Debugf("Get all deploymentUnitPool: %v from host: %s", allDeployments, host)

		for _, deployment := range allDeployments {
			for _, con := range context {
				logrus.Debugf("Get Contxt deploymentUnitUUID: %s", con.DeploymentUnitUUID)
				if con.DeploymentUnitUUID == deployment {
					matchHost = host
					break breakLabel
				}
			}
		}
	}
	if matchHost != "" {
		logrus.Debugf("VpcSubnetFilter match host: %s", matchHost)
		return []string{matchHost}
	}
	return hosts
}

type VpcSubnetReleaseAction struct{}

func (v VpcSubnetReleaseAction) Release(scheduler *Scheduler, requests []ResourceRequest, context Context, host *host) {
	logrus.Debugf("Release context: %+v", context)
	if context != nil && len(context) > 0 {
		dPool, ok := scheduler.hosts[host.id].pools[tempDeploymentUnitPool]
		if !ok {
			logrus.Infof("Host %v doesn't have resource pool tempDeploymentUnitPool. Nothing to do.", host.id)
			return
		}
		pool := dPool.(*DeploymentUnitPool)
		for _, cont := range context {
			deployments := append(pool.Deployments, cont.DeploymentUnitUUID)
			deployments = removeDuplicates(deployments)
			pool.Deployments = deployments
		}
		logrus.Debugf("Host:%s DeploymentUnitPool: %v", host.id, scheduler.hosts[host.id].pools[tempDeploymentUnitPool])
	}
}

type VpcSubnetReserveAction struct{}

func (v *VpcSubnetReserveAction) Reserve(scheduler *Scheduler, requests []ResourceRequest, context Context, host *host, force bool, data map[string]interface{}) error {
	logrus.Debugf("Reserve context: %+v", context)
	if context != nil && len(context) > 0 {
		dPool, ok := scheduler.hosts[host.id].pools[tempDeploymentUnitPool]
		if !ok {
			logrus.Infof("Host %v doesn't have resource pool tempDeploymentUnitPool. Nothing to do.", host.id)
			return nil
		}
		deployments := dPool.(*DeploymentUnitPool).Deployments
		var matchIndex int
	breakLabel:
		for index, deployment := range deployments {
			for _, con := range context {
				logrus.Debugf("Get Contxt deploymentUnitUUID: %s", con.DeploymentUnitUUID)
				if con.DeploymentUnitUUID == deployment {
					matchIndex = index
					break breakLabel
				}
			}
		}
		if matchIndex > 0 {
			dPool.(*DeploymentUnitPool).Deployments = append(deployments[:matchIndex], deployments[matchIndex+1:]...)
		}
		logrus.Debugf("Host:%s DeploymentUnitPool: %v", host.id, scheduler.hosts[host.id].pools[tempDeploymentUnitPool])
	}
	return nil
}

func (v *VpcSubnetReserveAction) RollBack(scheduler *Scheduler, requests []ResourceRequest, context Context, host *host) {
}

func removeDuplicates(in []string) (out []string) {
	m := map[string]bool{}
	for _, v := range in {
		if _, found := m[v]; !found {
			out = append(out, v)
			m[v] = true
		}
	}
	return out
}
