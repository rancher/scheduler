package scheduler

import (
	"testing"

	"gopkg.in/check.v1"
)

// gocheck setup
func Test(t *testing.T) { check.TestingT(t) }

type SchedulerTestSuite struct{}

var _ = check.Suite(&SchedulerTestSuite{})

func (s *SchedulerTestSuite) SetUpSuite(c *check.C) {
	// Nothing to setup yet
}

type rezTest struct {
	id               string
	force            bool
	resourceRequests []ResourceRequest
	expected         []string
}

func (s *SchedulerTestSuite) TestReserveResource(c *check.C) {
	scheduler := NewScheduler()

	err := scheduler.CreateResourcePool("1", "memory", 3, 0)
	if err != nil {
		c.Fatal(err)
	}
	err = scheduler.CreateResourcePool("1", "storage.size", 7, 0)
	if err != nil {
		c.Fatal(err)
	}
	err = scheduler.CreateResourcePool("2", "memory", 6, 0)
	if err != nil {
		c.Fatal(err)
	}
	err = scheduler.CreateResourcePool("2", "storage.size", 5, 0)
	if err != nil {
		c.Fatal(err)
	}

	tests := []rezTest{
		// Host 2 has more memory for first iteration
		{"1", false, []ResourceRequest{{Amount: 1, Resource: "memory"}, {Amount: 1, Resource: "storage.size"}}, []string{"2", "1"}},

		// Host 2 still has more memory. Request has two memory resource requests
		{"2", false, []ResourceRequest{{Amount: 1, Resource: "memory"}, {Amount: 1, Resource: "memory"}, {Amount: 1, Resource: "storage.size"}}, []string{"2", "1"}},

		// Host 2 and 1 memory are equal and host 1 has more storage
		{"3", false, []ResourceRequest{{Amount: 1, Resource: "memory"}, {Amount: 1, Resource: "storage.size"}}, []string{"1", "2"}},

		// Host 1 again has less memory
		{"4", false, []ResourceRequest{{Amount: 1, Resource: "memory"}, {Amount: 1, Resource: "storage.size"}}, []string{"2", "1"}},

		// Memory once again equal and host 1 has more storage, use up the rest of host 1's memory
		{"5", false, []ResourceRequest{{Amount: 2, Resource: "memory"}, {Amount: 1, Resource: "storage.size"}}, []string{"1", "2"}},

		// Only host 2 is left and use up all its storage
		{"6", false, []ResourceRequest{{Amount: 1, Resource: "memory"}, {Amount: 2, Resource: "storage.size"}}, []string{"2"}},

		// No hosts left
		{"8", false, []ResourceRequest{{Amount: 1, Resource: "memory"}, {Amount: 1, Resource: "storage.size"}}, []string{}},

		// Host 2 still has memory
		{"9", false, []ResourceRequest{{Amount: 1, Resource: "memory"}}, []string{"2"}},

		// Host 1 still has storage
		{"10", false, []ResourceRequest{{Amount: 1, Resource: "storage.size"}}, []string{"1"}},
	}
	checkPrioritizationAndReserve(scheduler, tests, c)

	// Release the resources to put host 1 back to mem=1, storage.size=4, and host 2 to mem=1, storage.size=1
	scheduler.ReleaseResources("1", []ResourceRequest{{Amount: 1, Resource: "memory"}})
	scheduler.ReleaseResources("2", []ResourceRequest{{Amount: 1, Resource: "memory"}, {Amount: 1, Resource: "storage.size"}})

	tests = []rezTest{
		// Host 1 has same memory and more storage
		{"1", false, []ResourceRequest{{Amount: 1, Resource: "memory"}, {Amount: 1, Resource: "storage.size"}}, []string{"1", "2"}},
		// Host 1 out of memory
		{"2", false, []ResourceRequest{{Amount: 1, Resource: "memory"}, {Amount: 1, Resource: "storage.size"}}, []string{"2"}},
	}
	checkPrioritizationAndReserve(scheduler, tests, c)
}

func (s *SchedulerTestSuite) TestForceReserve(c *check.C) {
	scheduler := &Scheduler{
		hosts: map[string]*host{},
	}
	err := scheduler.CreateResourcePool("1", "memory", 1, 0)
	c.Check(err, check.IsNil)

	err = scheduler.ReserveResources("1", true, []ResourceRequest{{Amount: 10, Resource: "memory"}})
	c.Assert(err, check.IsNil)
	c.Assert(scheduler.hosts["1"].pools["memory"].used, check.Equals, int64(10)) // assert that we over-committed
}

func (s *SchedulerTestSuite) TestRemoveHost(c *check.C) {
	scheduler := &Scheduler{
		hosts: map[string]*host{},
	}
	err := scheduler.CreateResourcePool("1", "memory", 1, 0)
	c.Check(err, check.IsNil)
	err = scheduler.CreateResourcePool("2", "memory", 2, 0)
	c.Check(err, check.IsNil)
	err = scheduler.CreateResourcePool("3", "memory", 3, 0)
	c.Check(err, check.IsNil)

	tests := []rezTest{{"1", false, []ResourceRequest{{Amount: 1, Resource: "memory"}}, []string{"3", "2", "1"}}}
	checkPrioritizationAndReserve(scheduler, tests, c)

	scheduler.RemoveHost("2")

	checkPrioritizationAndReserve(scheduler, []rezTest{{"1", false, []ResourceRequest{{Amount: 1, Resource: "memory"}}, []string{"3", "1"}}}, c)
}

func (s *SchedulerTestSuite) TestBadResourceReservation(c *check.C) {
	scheduler := &Scheduler{
		hosts: map[string]*host{},
	}
	err := scheduler.CreateResourcePool("1", "memory", 3, 0)
	if err != nil {
		c.Fatal(err)
	}
	err = scheduler.CreateResourcePool("1", "storage.size", 2, 0)
	if err != nil {
		c.Fatal(err)
	}

	// pool already on host, should just get updated
	err = scheduler.CreateResourcePool("1", "storage.size", 7, 0)
	c.Check(err, check.ErrorMatches, ".*already exists.*")

	// host doesn't exist
	err = scheduler.ReserveResources("2", false, []ResourceRequest{{Amount: 1, Resource: "memory"}})
	c.Assert(err, check.NotNil)
	err = scheduler.ReleaseResources("2", []ResourceRequest{{Amount: 1, Resource: "memory"}})
	c.Assert(err, check.NotNil)

	// pool doesn't exist on host
	err = scheduler.ReserveResources("1", false, []ResourceRequest{{Amount: 1, Resource: "network"}})
	c.Assert(err, check.NotNil)
	err = scheduler.ReleaseResources("1", []ResourceRequest{{Amount: 1, Resource: "network"}})
	c.Assert(err, check.NotNil)

	// Can't reserve more than is available
	err = scheduler.ReserveResources("1", false, []ResourceRequest{{Amount: 1, Resource: "storage.size"}, {Amount: 1, Resource: "memory"}})
	c.Assert(err, check.IsNil)
	err = scheduler.ReserveResources("1", false, []ResourceRequest{{Amount: 1, Resource: "storage.size"}, {Amount: 4, Resource: "memory"}})
	c.Assert(err, check.FitsTypeOf, OverReserveError{})
	c.Assert(err, check.ErrorMatches, ".*memory.*")
	// Assert that we rollback correctly
	c.Assert(scheduler.hosts["1"].pools["storage.size"].used, check.Equals, int64(1))
	c.Assert(scheduler.hosts["1"].pools["memory"].used, check.Equals, int64(1))

	// Can't release to above total
	err = scheduler.ReleaseResources("1", []ResourceRequest{{Amount: 1, Resource: "storage.size"}, {Amount: 4, Resource: "memory"}})
	c.Assert(err, check.FitsTypeOf, OverReleaseError{})
	c.Assert(err, check.ErrorMatches, ".*memory.*")
}

func checkPrioritizationAndReserve(scheduler *Scheduler, tests []rezTest, c *check.C) {
	for _, t := range tests {
		actual, err := scheduler.PrioritizeCandidates(t.resourceRequests)
		if err != nil {
			c.Fatal(err)
		}
		c.Logf("Checking %v", t.id)
		c.Assert(actual, check.DeepEquals, t.expected)

		if len(t.expected) > 0 {
			err = scheduler.ReserveResources(t.expected[0], t.force, t.resourceRequests)
			c.Assert(err, check.IsNil)
		}
	}
}
