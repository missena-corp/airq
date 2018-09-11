package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/missena-corp/airq"
	"github.com/missena-corp/airq/client"
	"google.golang.org/grpc"
)

func main() {
	conn, err := grpc.Dial(":8888", grpc.WithInsecure())
	if err != nil {
		fmt.Printf("could not connect to backend: %v\n", err)
		os.Exit(1)
	}
	cli := client.New(conn)
	cli.Push(context.Background(), []*airq.Job{
		&airq.Job{
			ID:      "foo",
			Content: "bar",
			When:    time.Unix(0, 1),
		},
		&airq.Job{
			ID:      "baz",
			Content: "qux",
			When:    time.Unix(0, 1),
		},
	}...)
	cli.Remove(context.Background(), "foo")
}
