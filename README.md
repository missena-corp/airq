# Air Q (**r**edis **q**ueue)

## What and why

This is a redis-based queue for usage in Go.
This project is a fork from [go-redis-queue](https://github.com/agilebits/go-redis-queue)
I liked this solution a lot but i missed a few features.

## Original Features

- Ability to add arbitrary tasks to a queue in redis
- Option to Dedup tasks based on the task signature.
- Ability to schedule tasks in the future.
- Atomic Push and Pop from queue. Two workers cannot get the same job.
- Sorted FIFO queue.
- Can act like a priority queue by scheduling a job with a really old timestamp
- Simple API

## Added Features

- Ability to remove a job
- Ability to have multiple times the same job (same body)

## Usage

Adding jobs to a queue.

```go
import "github.com/missena-corp/airq"
```

```go
c, err := redis.Dial("tcp", "127.0.0.1:6379")
if err != nil { ... }
defer c.Close()

q := airq.New(c, "some_queue_name")

added, taskID, err := q.Push(&airq.Job{Body: "basic item"})
if err != nil { ... }

queueSize, err := q.Pending()
if err != nil { ... }

added, taskID, err := q.Push(&airq.Job{
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

q := airq.New(c, "some_queue_name")

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

q := airq.New(c, "some_queue_name")

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
