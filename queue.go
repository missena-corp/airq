package airq

import (
	"fmt"
	"time"

	"github.com/gomodule/redigo/redis"
)

// Queue holds a reference to a redis connection and a queue name.
type Queue struct {
	Conn redis.Conn
	Name string
}

// Loop over the queue
func (q *Queue) Loop(cb func([]string, error)) {
	for {
		jobs, err := q.PopJobs(100)
		if err != nil || len(jobs) > 0 {
			cb(jobs, err)
			continue
		}
		time.Sleep(1 * time.Second)
	}
}

// New defines a new Queue
func New(c redis.Conn, name string) *Queue { return &Queue{Conn: c, Name: name} }

// Push schedule a job at some point in the future, or some point in the past.
// Scheduling a job far in the past is the same as giving it a high priority,
// as jobs are popped in order of due date.
func (q *Queue) Push(jobs ...*Job) (ids []string, err error) {
	if len(jobs) == 0 {
		return []string{}, fmt.Errorf("no jobs provided")
	}
	// keysAndArgs := []string{q.Name}
	keysAndArgs := redis.Args{q.Name}
	for _, j := range jobs {
		keysAndArgs = keysAndArgs.AddFlat(j.String())
		ids = append(ids, j.ID)
	}
	ok, err := redis.Int(pushScript.Do(q.Conn, keysAndArgs...))
	if err == nil && ok != 1 {
		err = fmt.Errorf("can't add all jobs %v to queue %s", jobs, q.Name)
	}
	return ids, err
}

// Pending returns the count of jobs pending, including scheduled jobs that are not due yet.
func (q *Queue) Pending() (int64, error) { return redis.Int64(q.Conn.Do("ZCARD", q.Name)) }

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
func (q *Queue) PopJobs(limit int) (res []string, err error) {
	if limit == 0 {
		return []string{}, fmt.Errorf("limit 0")
	}
	redisRes, err := redis.Strings(popJobsScript.Do(
		q.Conn, q.Name, time.Now().UnixNano(), limit,
	))
	if err != nil {
		return nil, err
	}
	for _, c := range redisRes {
		res = append(res, uncompress(c))
	}
	return res, nil
}

// Remove removes a job from the queue
func (q *Queue) Remove(ids ...string) error {
	if len(ids) == 0 {
		return fmt.Errorf("no id provided")
	}
	ok, err := redis.Int(removeScript.Do(q.Conn, redis.Args{q.Name}.AddFlat(ids)...))
	if err == nil && ok != 1 {
		err = fmt.Errorf("can't delete all jobs %v in queue %s", ids, q.Name)
	}
	return err
}
