package main

import (
	"log"

	"github.com/gomodule/redigo/redis"
	"github.com/missena-corp/airq"
	"github.com/missena-corp/airq/server"
)

func main() {
	c, _ := redis.Dial("tcp", "127.0.0.1:6379")
	defer c.Close()

	q := airq.New(c, "queue_name")
	srv := server.New(q)

	log.Fatal(srv.Serve(":8888"))
}
