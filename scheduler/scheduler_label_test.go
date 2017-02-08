package scheduler

import check "gopkg.in/check.v1"

func (s *SchedulerTestSuite) TestLabelFilter(c *check.C) {
	scheduler := NewScheduler(-1)

	err := scheduler.CreateResourcePool("1", &LabelPool{
		Resource: "hostLabels",
		Labels: map[string]string{
			requireAnyLabel: "foo=bar,foo1=bar1",
		},
	})
	if err != nil {
		c.Fatal(err)
	}
	err = scheduler.CreateResourcePool("2", &LabelPool{
		Resource: "hostLabels",
		Labels: map[string]string{
			requireAnyLabel: "foo2=bar2,foo3=bar3",
		},
	})
	if err != nil {
		c.Fatal(err)
	}
	con1 := contextStruct{}
	con1.Data.Fields.Labels = map[string]string{
		"foo": "bar",
	}
	context1 := Context{con1}

	con2 := contextStruct{}
	con2.Data.Fields.Labels = map[string]string{
		"foo1": "bar1",
	}
	context2 := Context{con2}

	con3 := contextStruct{}
	con3.Data.Fields.Labels = map[string]string{
		"foo2": "bar2",
	}
	context3 := Context{con3}

	con4 := contextStruct{}
	con4.Data.Fields.Labels = map[string]string{
		"foo3": "bar3",
	}
	context4 := Context{con4}

	con5 := contextStruct{}
	con5.Data.Fields.Labels = map[string]string{
		"foo4": "bar4",
	}
	context5 := Context{con5}

	actual, err := scheduler.PrioritizeCandidates(nil, context1)
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{"1"})

	actual, err = scheduler.PrioritizeCandidates(nil, context2)
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{"1"})

	actual, err = scheduler.PrioritizeCandidates(nil, context3)
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{"2"})

	actual, err = scheduler.PrioritizeCandidates(nil, context4)
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{"2"})

	actual, err = scheduler.PrioritizeCandidates(nil, context5)
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{})
}
