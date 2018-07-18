package redisqueue

import (
	"crypto/rand"
	"reflect"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
)

func initQueue(t *testing.T) *Queue {
	name := randomName()
	c, err := redis.Dial("tcp", "127.0.0.1:6379")
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	q := New(name, c)
	if err := flushQueue(q); err != nil {
		t.Error(err)
		t.FailNow()
	}
	return q
}

func addJobs(t *testing.T, q *Queue, jobs []Job) {
	for _, job := range jobs {
		if _, _, err := q.Push(job); err != nil {
			t.Error(err)
			t.FailNow()
		}
	}
}

func clear(q *Queue) {
	flushQueue(q)
	q.c.Close()
}

func flushQueue(q *Queue) error {
	q.c.Send("MULTI")
	q.c.Send("DEL", q.KeyQueue)
	q.c.Send("DEL", q.ValueQueue)
	_, err := q.c.Do("EXEC")
	return err
}

func randomName() string {
	b := make([]byte, 12)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}
	return string(b)
}

func TestQueueTasks(t *testing.T) {
	t.Parallel()
	q := initQueue(t)
	defer clear(q)

	b, _, err := q.Push(Job{Content: "basic item 1"})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if b != true {
		t.Error("expected item to be added to queue but was not")
	}

	b, _, err = q.Push(Job{Content: "basic item 1"})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if b != false {
		t.Error("expected item not to be added to queue but it was")
	}

	pending, err := q.Pending()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if pending != 1 {
		t.Error("Expected 1 job pending in queue, was", pending)
	}
}

func TestQueueTaskScheduling(t *testing.T) {
	t.Parallel()
	q := initQueue(t)
	defer clear(q)

	b, _, err := q.Push(Job{Content: "scheduled item 1", When: time.Now().Add(90 * time.Millisecond)})
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if b != true {
		t.Error("expected item to be added to queue but was not")
	}

	pending, err := q.Pending()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if pending != 1 {
		t.Error("Expected 1 job pending in queue, was", pending)
	}

	job, err := q.Pop()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if job != "" {
		t.Error("Didn't expect to get a job off the queue but I got one.")
	}

	// Wait for the job to become ready.
	time.Sleep(100 * time.Millisecond)

	job, err = q.Pop()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if job != "scheduled item 1" {
		t.Error("Expected to get a job off the queue, but I got this:", job)
	}
}

func TestPopOrder(t *testing.T) {
	t.Parallel()
	q := initQueue(t)
	defer clear(q)

	addJobs(t, q, []Job{
		Job{Content: "oldest", When: time.Now().Add(-300 * time.Millisecond)},
		Job{Content: "newer", When: time.Now().Add(-100 * time.Millisecond)},
		Job{Content: "older", When: time.Now().Add(-200 * time.Millisecond)},
	})

	job, err := q.Pop()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if job != "oldest" {
		t.Error("Expected to the oldest job off the queue, but I got this:", job)
	}

	job, err = q.Pop()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if job != "older" {
		t.Error("Expected to the older job off the queue, but I got this:", job)
	}

	job, err = q.Pop()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	if job != "newer" {
		t.Error("Expected to the newer job off the queue, but I got this:", job)
	}

	job, err = q.Pop()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if job != "" {
		t.Error("Expected no jobs")
	}
}

func TestPopMultiOrder(t *testing.T) {
	t.Parallel()
	q := initQueue(t)
	defer clear(q)

	addJobs(t, q, []Job{
		Job{Content: "oldest", When: time.Now().Add(-300 * time.Millisecond)},
		Job{Content: "newer", When: time.Now().Add(-100 * time.Millisecond)},
		Job{Content: "older", When: time.Now().Add(-200 * time.Millisecond)},
	})

	jobs, err := q.PopJobs(3)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	expected := []string{"oldest", "older", "newer"}
	if !reflect.DeepEqual(jobs, expected) {
		t.Error("Expected to the oldest job off the queue, but I got this:", jobs)
	}

	job, err := q.Pop()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if job != "" {
		t.Error("Expected no jobs")
	}
}

func TestRemove(t *testing.T) {
	t.Parallel()
	q := initQueue(t)
	defer clear(q)

	addJobs(t, q, []Job{
		Job{Content: "oldest", When: time.Now().Add(-300 * time.Millisecond)},
		Job{Content: "newer", When: time.Now().Add(-100 * time.Millisecond)},
		Job{Content: "older", When: time.Now().Add(-200 * time.Millisecond), ID: "OLDER_ID"},
	})

	q.Remove("OLDER_ID")

	jobs, err := q.PopJobs(3)
	if err != nil {
		t.Error(err)
		t.FailNow()
	}

	expected := []string{"oldest", "newer"}
	if !reflect.DeepEqual(jobs, expected) {
		t.Error("Expected to the oldest job off the queue, but I got this:", jobs)
	}

	job, err := q.Pop()
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	if job != "" {
		t.Error("Expected no jobs")
	}
}
