package client

import (
	"context"

	"github.com/missena-corp/airq"
	"github.com/missena-corp/airq/job"
	"google.golang.org/grpc"
)

type Client struct {
	Conn *grpc.ClientConn
}

func New(conn *grpc.ClientConn) *Client {
	return &Client{Conn: conn}
}

func (c *Client) Push(ctx context.Context, jobs ...*airq.Job) (*job.IdList, error) {
	if len(jobs) == 0 {
		return new(job.IdList), nil
	}
	jobList := new(job.JobList)
	for _, j := range jobs {
		jobList.Jobs = append(jobList.Jobs, &job.Job{
			Id:      j.ID,
			Content: j.Content,
			Unique:  j.Unique,
			When:    j.When.UnixNano(),
		})
	}
	client := job.NewJobsClient(c.Conn)
	return client.Push(ctx, jobList)
}

func (c *Client) Remove(ctx context.Context, ids ...string) error {
	if len(ids) == 0 {
		return nil
	}
	idList := new(job.IdList)
	for _, i := range ids {
		idList.Ids = append(idList.Ids, &job.Id{Id: i})
	}
	client := job.NewJobsClient(c.Conn)
	_, err := client.Remove(ctx, idList)
	return err
}
