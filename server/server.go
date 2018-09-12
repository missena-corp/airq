package server

import (
	"context"
	"net"
	"time"

	"github.com/missena-corp/airq"
	"github.com/missena-corp/airq/job"
	"google.golang.org/grpc"
)

type Server struct {
	*grpc.Server
	Queue *airq.Queue
}

func New(q *airq.Queue) Server {
	return Server{
		Server: grpc.NewServer(),
		Queue:  q,
	}
}

func (s Server) Serve(connStr string) error {
	job.RegisterJobsServer(s.Server, s)
	l, err := net.Listen("tcp", connStr)
	if err != nil {
		return err
	}
	return s.Server.Serve(l)
}

func (s Server) Push(ctx context.Context, jobList *job.JobList) (*job.IdList, error) {
	var jobs []*airq.Job
	idList := new(job.IdList)
	for _, j := range jobList.Jobs {
		jobs = append(jobs, &airq.Job{
			ID:      j.GetId(),
			Content: j.GetContent(),
			Unique:  j.GetUnique(),
			When:    time.Unix(0, j.GetWhen()),
		})
	}
	ids, err := s.Queue.Push(jobs...)
	if err != nil || len(ids) == 0 {
		return idList, err
	}
	for _, id := range ids {
		idList.Ids = append(idList.Ids, &job.Id{Id: id})
	}
	return idList, nil
}

func (s Server) Remove(ctx context.Context, jobs *job.IdList) (*job.Void, error) {
	var ids []string
	for _, i := range jobs.GetIds() {
		ids = append(ids, i.Id)
	}
	return &job.Void{}, s.Queue.Remove(ids...)
}
