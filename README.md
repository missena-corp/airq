# go-redis-queue

## What and why

This is a redis-based queue for usage in Go. I evaluated a lot of other options before writing this, but I really didn't like the API of most of the other options out there, and nearly all of them were missing one or more of my required features.

## Features

- Ability to add arbitrary tasks to a queue in redis
- Option to Dedup tasks based on the task signature.
- Ability to schedule tasks in the future.
- Atomic Push and Pop from queue. Two workers cannot get the same job.
- Sorted FIFO queue.
- Can act like a priority queue by scheduling a job with a really old timestamp
- Well tested
- Small, concise codebase
- Simple API

## Usage

Adding jobs to a queue.

```go
import "github.com/AgileBits/go-redis-queue/redisqueue"
```

```go
c, err := redis.Dial("tcp", "127.0.0.1:6379")
if err != nil { ... }
defer c.Close()

q := redisqueue.New("some_queue_name", c)

wasAdded, taskID, err := q.Push(redisqueue.Job{Body: "basic item"})
if err != nil { ... }

queueSize, err := q.Pending()
if err != nil { ... }

wasAdded, taskID, err := q.Push(redisqueue.Job{
  Body: "scheduled item",
  When: time.Now().Add(10*time.Minute),
})
if err != nil { ... }
```

A simple worker processing jobs from a queue:

```go
c, err := redis.Dial("tcp", "127.0.0.1:6379")
if err != nil { ... }
defer c.Close()

q := redisqueue.New("some_queue_name", c)

for !timeToQuit {
  job, err = q.Pop()
  if err != nil { ... }
  if job != "" {
    // process the job.
  } else {
    time.Sleep(2*time.Second)
  }
}
```

A batch worker processing jobs from a queue:

```go
c, err := redis.Dial("tcp", "127.0.0.1:6379")
if err != nil { ... }
defer c.Close()

q := redisqueue.New("some_queue_name", c)

for !timeToQuit {
  jobs, err := q.PopJobs(100) // argument is "limit"
  if err != nil { ... }
  if len(jobs) > 0 {
    for i, job := range jobs {
      // process the job.
    }
  } else {
    time.Sleep(2*time.Second)
  }
}
```

## Requirements

- Redis 2.6.0 or greater
- github.com/gomodule/redigo/redis
- Go
