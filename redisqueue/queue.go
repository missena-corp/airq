package redisqueue

import (
	"crypto/rand"
	"crypto/sha1"
	"encoding/base64"
	"encoding/hex"
	"io"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
)

// Job is the struct of job in queue
type Job struct {
	Body   string
	ID     string
	Unique bool
	When   time.Time
}

// Queue holds a reference to a redis connection and a queue name.
type Queue struct {
	c          redis.Conn
	KeyQueue   string
	ValueQueue string
}

func (j *Job) generateID() string {
	if j.Unique {
		b := make([]byte, 40)
		rand.Read(b)
		return base64.URLEncoding.EncodeToString(b)
	}
	h := sha1.New()
	io.WriteString(h, j.Body)
	return hex.EncodeToString(h.Sum(nil))
}

// New defines a full job
func (j *Job) New() *Job {
	if j.ID == "" {
		j.ID = j.generateID()
	}
	if j.When.IsZero() {
		j.When = time.Now()
	}
	return j
}

// New defines a new Queue
func New(name string, c redis.Conn) *Queue {
	return &Queue{
		c:          c,
		KeyQueue:   name,
		ValueQueue: name + ":values",
	}
}

// Remove removes a job from the queue
func (q *Queue) Remove(id string) (bool, error) {
	ok, err := redis.Int(removeScript.Do(q.c, q.KeyQueue, id))
	return ok == 1, err
}

// Push schedule a job at some point in the future, or some point in the past.
// Scheduling a job far in the past is the same as giving it a high priority,
// as jobs are popped in order of due date.
func (q *Queue) Push(j *Job) (bool, string, error) {
	j = j.New()
	ok, err := redis.Int(pushScript.Do(q.c, q.KeyQueue, j.When.UnixNano(), j.ID, j.Body))
	return ok == 1, j.ID, err
}

// Pending returns the count of jobs pending, including scheduled jobs that are not due yet.
func (q *Queue) Pending() (int64, error) {
	return redis.Int64(q.c.Do("ZCARD", q.KeyQueue))
}

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
