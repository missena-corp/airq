package airq_test

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/missena-corp/airq"
	"github.com/missena-corp/airq/client"
	"github.com/missena-corp/airq/server"
	"google.golang.org/grpc"
)

func newPool() *redis.Pool {
	return &redis.Pool{
		MaxIdle:   8,
		MaxActive: 12,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", ":6379")
			if err != nil {
				panic(err.Error())
			}
			return c, err
		},
	}
}

func randomName() string {
	b := make([]byte, 12)
	rand.Read(b)
	return base64.URLEncoding.EncodeToString(b)
}

func setup(t *testing.T) (*airq.Queue, func()) {
	t.Parallel()
	name := randomName()
	q := airq.New(name, airq.WithPool(newPool()))
	teardown := func() {
		conn := q.Pool.Get()
		conn.Send("DEL", q.Name)
		conn.Send("DEL", q.Name+":values")
		conn.Close()
	}
	return q, teardown
}

func TestService(t *testing.T) {
	q, teardown := setup(t)
	defer teardown()

	connStr := ":42039"

	srv := server.New(q)
	go srv.Serve(connStr)
	defer srv.Stop()
	// wait for the grpc server to be up
	time.Sleep(200 * time.Millisecond)

	conn, err := grpc.Dial(connStr, grpc.WithInsecure())
	if err != nil {
		t.Error(err)
		t.FailNow()
	}
	cli := client.New(conn)
	idList, err := cli.Push(context.Background(), []*airq.Job{
		&airq.Job{ID: "foo", Content: "bar", When: time.Unix(0, 1)},
		&airq.Job{ID: "baz", Content: "qux", When: time.Unix(0, 2)},
	}...)
	if err != nil {
		t.Error(err)
	}
	if len(idList.Ids) != 2 {
		t.Error("2 ids should have been generated")
	}
	if err := cli.Remove(context.Background(), "foo"); err != nil {
		t.Error(err)
	}
}
