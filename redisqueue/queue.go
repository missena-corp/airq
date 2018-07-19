package redisqueue

import (
	"crypto/md5"
	"crypto/rand"
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

func generateID(content string) string {
	h := md5.New()
	io.WriteString(h, content)
	return hex.EncodeToString(h.Sum(nil))
}

func generateRandomID() string {
	b := make([]byte, 32)
	rand.Read(b)
	return base64.URLEncoding.EncodeToString(b)
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
func (q *Queue) Push(job Job) (bool, string, error) {
	if job.ID == "" {
		if job.Unique {
			job.ID = generateRandomID()
		} else {
			job.ID = generateID(job.Body)
		}
	}
	if job.When.IsZero() {
		job.When = time.Now()
	}
	ok, err := redis.Int(pushScript.Do(
		q.c, q.KeyQueue, job.When.UnixNano(), job.ID, job.Body,
	))
	return ok == 1, job.ID, err
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
