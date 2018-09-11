package client

import (
	"context"

	"github.com/missena-corp/airq"
	"github.com/missena-corp/airq/job"
	"google.golang.org/grpc"
)

// conn, err := grpc.Dial(":8888", grpc.WithInsecure())
// if err != nil {
// 	fmt.Printf("could not connect to backend: %v\n", err)
// 	os.Exit(1)
// }
// client := job.NewJobsClient(conn)
// client.Push(context.Background(), &job.JobList{
// 	Jobs: []*job.Job{
// 		&job.Job{
// 			Id:      "foo",
// 			Content: "bar",
// 			When:    1,
// 		},
// 		&job.Job{
// 			Id:      "baz",
// 			Content: "qux",
// 			When:    1,
// 		},
// 	},
// })

// client.Remove(context.Background(), &job.IdList{
// 	Ids: []*job.Id{&job.Id{Id: "foo"}},
// })

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
	var jobList *job.JobList
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

func (c *Client) Remove(ctx context.Context, ids ...string) (*job.Void, error) {
	if len(ids) == 0 {
		return new(job.Void), nil
	}
	var idList *job.IdList
	for _, i := range ids {
		idList.Ids = append(idList.Ids, &job.Id{Id: i})
	}
	client := job.NewJobsClient(c.Conn)
	return client.Remove(ctx, idList)
}
