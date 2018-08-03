package airq

import (
	"fmt"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
)

// Queue holds a reference to a redis connection and a queue name.
type Queue struct {
	c          redis.Conn
	KeyQueue   string
	ValueQueue string
}

// New defines a new Queue
func New(c redis.Conn, name string) *Queue {
	return &Queue{
		c:          c,
		KeyQueue:   name,
		ValueQueue: name + ":values",
	}
}

// Remove removes a job from the queue
func (q *Queue) Remove(ids ...string) error {
	keysAndArgs := append([]string{q.KeyQueue}, ids...)
	ok, err := redis.Int(removeScript.Do(q.c, toInterface(keysAndArgs)...))
	if err == nil && ok != 1 {
		err = fmt.Errorf("error while deleting jobs")
	}
	return err
}

// Push schedule a job at some point in the future, or some point in the past.
// Scheduling a job far in the past is the same as giving it a high priority,
// as jobs are popped in order of due date.
func (q *Queue) Push(jobs ...*Job) (ids []string, err error) {
	keysAndArgs := []string{q.KeyQueue}
	for _, j := range jobs {
		keysAndArgs = append(keysAndArgs, j.String())
		ids = append(ids, j.ID)
	}
	ok, err := redis.Int(pushScript.Do(q.c, toInterface(keysAndArgs)...))
	if err == nil && ok != 1 {
		err = fmt.Errorf("some jobs are not added")
	}
	return ids, err
}

// Pending returns the count of jobs pending, including scheduled jobs that are not due yet.
func (q *Queue) Pending() (int64, error) { return redis.Int64(q.c.Do("ZCARD", q.KeyQueue)) }

// Pop removes and returns a single job from the queue. Safe for concurrent use
// (multiple goroutines must use their own Queue objects and redis connections)
func (q *Queue) Pop() (string, error) {
	jobs, err := q.PopJobs(1)
	if err != nil {
		return "", err
	}
	if len(jobs) == 0 {
		return "", nil
	}
	return jobs[0], nil
}

// PopJobs returns multiple jobs from the queue. Safe for concurrent use
// (multiple goroutines must use their own Queue objects and redis connections)
func (q *Queue) PopJobs(limit int) ([]string, error) {
	return redis.Strings(popJobsScript.Do(
		q.c, q.KeyQueue, time.Now().UnixNano(), strconv.Itoa(limit),
	))
}

func toInterface(strs []string) []interface{} {
	intrs := make([]interface{}, len(strs))
	for i := range strs {
		intrs[i] = strs[i]
	}
	return intrs
}
