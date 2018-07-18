package redisqueue

import (
	"crypto/md5"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"time"

	"github.com/gomodule/redigo/redis"
)

type Job struct {
	ID      string
	Content string
	When    time.Time
}

// Queue holds a reference to a redis connection and a queue name.
type Queue struct {
	c          redis.Conn
	KeyQueue   string
	ValueQueue string
}

func bothUpdated(reply interface{}) bool {
	s := reflect.ValueOf(reply)
	if s.Kind() != reflect.Slice {
		return false
	}
	res := make([]interface{}, s.Len())
	for i := 0; i < s.Len(); i++ {
		res[i] = s.Index(i).Interface()
	}
	val0, _ := res[0].(int64)
	val1, _ := res[1].(int64)
	return len(res) == 2 && val0 == 1 && val1 == 1
}

func generateID(content string) string {
	h := md5.New()
	io.WriteString(h, content)
	return string(h.Sum(nil))
}

// New defines a new Queue
func New(name string, c redis.Conn) *Queue {
	return &Queue{
		c:          c,
		KeyQueue:   name,
		ValueQueue: name + ":values",
	}
}

// Remove removes a fob from the queue
func (q *Queue) Remove(id string) (bool, error) {
	q.c.Send("MULTI")
	q.c.Send("ZREM", q.KeyQueue, id)
	q.c.Send("HDEL", q.ValueQueue, id)
	return redis.Bool(q.c.Do("EXEC"))
}

// Push schedule a job at some point in the future, or some point in the past.
// Scheduling a job far in the past is the same as giving it a high priority,
// as jobs are popped in order of due date.
func (q *Queue) Push(job Job) (bool, error) {
	if job.ID == "" {
		job.ID = generateID(job.Content)
	}
	if job.When.IsZero() {
		job.When = time.Now()
	}
	q.c.Send("MULTI")
	q.c.Send("ZADD", q.KeyQueue, job.When.UnixNano(), job.ID)
	q.c.Send("HSET", q.ValueQueue, job.ID, job.Content)
	reply, err := q.c.Do("EXEC")
	return bothUpdated(reply), err
}

// Pending returns the count of jobs pending, including scheduled jobs that are not due yet.
func (q *Queue) Pending() (int64, error) {
	return redis.Int64(q.c.Do("ZCARD", q.KeyQueue))
}

// FlushQueue removes everything from the queue. Useful for testing.
func (q *Queue) FlushQueue() error {
	q.c.Send("MULTI")
	q.c.Send("DEL", q.KeyQueue)
	q.c.Send("DEL", q.ValueQueue)
	_, err := q.c.Do("EXEC")
	return err
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
	return redis.Strings(popJobsScript.Do(q.c, q.KeyQueue, fmt.Sprintf("%d", time.Now().UnixNano()), strconv.Itoa(limit)))
}
