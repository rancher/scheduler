package scheduler

import check "gopkg.in/check.v1"

func (s *SchedulerTestSuite) TestLabelFilter(c *check.C) {
	scheduler := NewScheduler(-1)

	err := scheduler.CreateResourcePool("1", &LabelPool{
		Resource: "hostLabels",
		Labels: map[string]string{
			requireAnyLabel: "foo=bar,foo1=bar1,foo5",
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

	context6 := Context{con1, con2}

	context7 := Context{con1, con5}

	con8 := contextStruct{}
	con8.Data.Fields.Labels = map[string]string{
		"foo5": "",
	}
	context8 := Context{con8}

	con9 := contextStruct{}
	con9.Data.Fields.Labels = map[string]string{
		"foo5": "bar",
	}
	context9 := Context{con9}

	context10 := Context{con2, con8}

	context11 := Context{con5, con8}

	con12 := contextStruct{}
	con12.Data.Fields.Labels = map[string]string{
		"foo4": "",
	}
	context12 := Context{con12}

	//test foo=bar
	actual, err := scheduler.PrioritizeCandidates(nil, context1)
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{"1"})

	//test foo1=bar1
	actual, err = scheduler.PrioritizeCandidates(nil, context2)
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{"1"})

	//test foo2=bar2
	actual, err = scheduler.PrioritizeCandidates(nil, context3)
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{"2"})

	//test foo3=bar3
	actual, err = scheduler.PrioritizeCandidates(nil, context4)
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{"2"})

	//test foo4=bar4
	actual, err = scheduler.PrioritizeCandidates(nil, context5)
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{})

	//test foo=bar, foo1=bar1
	actual, err = scheduler.PrioritizeCandidates(nil, context6)
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{"1"})

	//test foo=bar, foo4=bar4
	actual, err = scheduler.PrioritizeCandidates(nil, context7)
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{"1"})

	// test foo5=
	actual, err = scheduler.PrioritizeCandidates(nil, context8)
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{"1"})

	//test foo5=bar
	actual, err = scheduler.PrioritizeCandidates(nil, context9)
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{"1"})

	//test foo1=bar1, foo5=
	actual, err = scheduler.PrioritizeCandidates(nil, context10)
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{"1"})

	//test foo4=bar4, foo5=
	actual, err = scheduler.PrioritizeCandidates(nil, context11)
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{"1"})

	//test foo4=
	actual, err = scheduler.PrioritizeCandidates(nil, context12)
	c.Assert(err, check.IsNil)
	c.Assert(actual, check.DeepEquals, []string{})
}
