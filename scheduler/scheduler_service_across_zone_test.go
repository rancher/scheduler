package scheduler

import (
	check "gopkg.in/check.v1"
	"sort"
)

func (s *SchedulerTestSuite) TestServiceAcrossZoneEvenWeights(c *check.C) {
	scheduler := NewScheduler(-1)

	scheduler.hosts = map[string]*host{
		"host-a": {
			id: "host-a",
			pools: map[string]ResourcePool{
				"hostLabels": &LabelPool{
					Labels: map[string]string{
						"zone": "west-1",
					},
				},
			},
		},
		"host-b": {
			id: "host-b",
			pools: map[string]ResourcePool{
				"hostLabels": &LabelPool{
					Labels: map[string]string{
						"zone": "west-2",
					},
				},
			},
		},
		"host-c": {
			id: "host-c",
			pools: map[string]ResourcePool{
				"hostLabels": &LabelPool{
					Labels: map[string]string{
						"zone": "west-3",
					},
				},
			},
		},
	}

	con1 := contextStruct{}
	con1.Data.Fields.Labels = map[string]string{
		serviceLabel:     "test/service1",
		zoneSplitsLabels: "key=zone",
	}
	context1 := Context{con1}

	actual1, err := scheduler.PrioritizeCandidates(nil, context1)
	c.Assert(err, check.IsNil)
	scheduler.ReserveResources(actual1[0], false, nil, context1)
	c.Assert(scheduler.services["test/service1"].total, check.Equals, int64(1))

	actual2, err := scheduler.PrioritizeCandidates(nil, context1)
	c.Assert(err, check.IsNil)
	scheduler.ReserveResources(actual2[0], false, nil, context1)
	c.Assert(scheduler.services["test/service1"].total, check.Equals, int64(2))

	actual3, err := scheduler.PrioritizeCandidates(nil, context1)
	c.Assert(err, check.IsNil)
	scheduler.ReserveResources(actual3[0], false, nil, context1)
	c.Assert(scheduler.services["test/service1"].total, check.Equals, int64(3))

	total := []string{}
	total = append(total, actual1[0], actual2[0], actual3[0])
	c.Assert(total, check.HasLen, 3)
	sort.Strings(total)
	c.Assert(total, check.DeepEquals, []string{"host-a", "host-b", "host-c"})

	scheduler.ReleaseResources(actual1[0], nil, context1)
	scheduler.ReleaseResources(actual2[0], nil, context1)
	scheduler.ReleaseResources(actual2[0], nil, context1)
	c.Assert(scheduler.services["test/service1"].total, check.Equals, int64(0))
}

func (s *SchedulerTestSuite) TestServiceAcrossZoneDiffWeight(c *check.C) {
	scheduler := NewScheduler(-1)

	scheduler.hosts = map[string]*host{
		"host-a": {
			id: "host-a",
			pools: map[string]ResourcePool{
				"hostLabels": &LabelPool{
					Labels: map[string]string{
						"zone": "west-1",
					},
				},
			},
		},
		"host-b": {
			id: "host-b",
			pools: map[string]ResourcePool{
				"hostLabels": &LabelPool{
					Labels: map[string]string{
						"zone": "West-2",
					},
				},
			},
		},
		"host-c": {
			id: "host-c",
			pools: map[string]ResourcePool{
				"hostLabels": &LabelPool{
					Labels: map[string]string{
						"zone": "west-3",
					},
				},
			},
		},
		"host-d": {
			id: "host-d",
			pools: map[string]ResourcePool{
				"hostLabels": &LabelPool{
					Labels: map[string]string{},
				},
			},
		},
	}

	con1 := contextStruct{}
	con1.Data.Fields.Labels = map[string]string{
		serviceLabel:     "test/service1",
		zoneSplitsLabels: "key=zone; west-1=1, west-2=2, west-3=3",
	}
	context1 := Context{con1}

	actual1, err := scheduler.PrioritizeCandidates(nil, context1)
	c.Assert(err, check.IsNil)
	c.Assert(len(actual1), check.Equals, 3)
	scheduler.ReserveResources(actual1[0], false, nil, context1)
	c.Assert(scheduler.services["test/service1"].total, check.Equals, int64(1))

	actual2, err := scheduler.PrioritizeCandidates(nil, context1)
	c.Assert(err, check.IsNil)
	scheduler.ReserveResources(actual2[0], false, nil, context1)
	c.Assert(scheduler.services["test/service1"].total, check.Equals, int64(2))

	actual3, err := scheduler.PrioritizeCandidates(nil, context1)
	c.Assert(err, check.IsNil)
	scheduler.ReserveResources(actual3[0], false, nil, context1)
	c.Assert(scheduler.services["test/service1"].total, check.Equals, int64(3))

	actual4, err := scheduler.PrioritizeCandidates(nil, context1)
	c.Assert(err, check.IsNil)
	scheduler.ReserveResources(actual4[0], false, nil, context1)
	c.Assert(scheduler.services["test/service1"].total, check.Equals, int64(4))

	actual5, err := scheduler.PrioritizeCandidates(nil, context1)
	c.Assert(err, check.IsNil)
	scheduler.ReserveResources(actual5[0], false, nil, context1)
	c.Assert(scheduler.services["test/service1"].total, check.Equals, int64(5))

	actual6, err := scheduler.PrioritizeCandidates(nil, context1)
	c.Assert(err, check.IsNil)
	scheduler.ReserveResources(actual6[0], false, nil, context1)
	c.Assert(scheduler.services["test/service1"].total, check.Equals, int64(6))

	c.Assert(scheduler.services["test/service1"].currentDistribution["west-1"], check.Equals, int64(1))
	c.Assert(scheduler.services["test/service1"].currentDistribution["west-2"], check.Equals, int64(2))
	c.Assert(scheduler.services["test/service1"].currentDistribution["west-3"], check.Equals, int64(3))
}

func (s *SchedulerTestSuite) TestServiceAcrossZoneSoft(c *check.C) {
	scheduler := NewScheduler(-1)

	scheduler.hosts = map[string]*host{
		"host-a": {
			id: "host-a",
			pools: map[string]ResourcePool{
				"hostLabels": &LabelPool{
					Labels: map[string]string{
						"zone": "west-1",
					},
				},
			},
		},
	}

	con1 := contextStruct{}
	con1.Data.Fields.Labels = map[string]string{
		serviceLabel:     "test/service1",
		zoneSplitsLabels: "key=zone; west-1=1, west-2=2, west-3=3",
	}
	context1 := Context{con1}

	actual1, err := scheduler.PrioritizeCandidates(nil, context1)
	c.Assert(err, check.IsNil)
	scheduler.ReserveResources(actual1[0], false, nil, context1)
	c.Assert(scheduler.services["test/service1"].total, check.Equals, int64(1))

	actual2, err := scheduler.PrioritizeCandidates(nil, context1)
	c.Assert(err, check.IsNil)
	scheduler.ReserveResources(actual2[0], false, nil, context1)
	c.Assert(scheduler.services["test/service1"].total, check.Equals, int64(2))

	actual3, err := scheduler.PrioritizeCandidates(nil, context1)
	c.Assert(err, check.IsNil)
	scheduler.ReserveResources(actual3[0], false, nil, context1)
	c.Assert(scheduler.services["test/service1"].total, check.Equals, int64(3))

	actual4, err := scheduler.PrioritizeCandidates(nil, context1)
	c.Assert(err, check.IsNil)
	scheduler.ReserveResources(actual4[0], false, nil, context1)
	c.Assert(scheduler.services["test/service1"].total, check.Equals, int64(4))

	c.Assert(scheduler.services["test/service1"].currentDistribution["west-1"], check.Equals, int64(4))
}

func (s *SchedulerTestSuite) TestServiceAcrossZoneWithFakeReserve(c *check.C) {
	scheduler := NewScheduler(1)

	scheduler.hosts = map[string]*host{
		"host-a": {
			id: "host-a",
			pools: map[string]ResourcePool{
				"hostLabels": &LabelPool{
					Labels: map[string]string{
						"zone": "west-1",
					},
				},
			},
		},
		"host-b": {
			id: "host-b",
			pools: map[string]ResourcePool{
				"hostLabels": &LabelPool{
					Labels: map[string]string{
						"zone": "west-2",
					},
				},
			},
		},
	}

	con1 := contextStruct{}
	con1.Data.Fields.Labels = map[string]string{
		serviceLabel:     "test/service1",
		zoneSplitsLabels: "key=zone; west-1=1, west-2=3, west-3=4",
	}
	context1 := Context{con1}

	_, err := scheduler.PrioritizeCandidates(nil, context1)
	c.Assert(err, check.IsNil)

	_, err = scheduler.PrioritizeCandidates(nil, context1)
	c.Assert(err, check.IsNil)

	_, err = scheduler.PrioritizeCandidates(nil, context1)
	c.Assert(err, check.IsNil)

	_, err = scheduler.PrioritizeCandidates(nil, context1)
	c.Assert(err, check.IsNil)

	c.Assert(scheduler.services["test/service1"].currentDistribution["west-1"], check.Equals, int64(1))
	c.Assert(scheduler.services["test/service1"].currentDistribution["west-2"], check.Equals, int64(3))

}
