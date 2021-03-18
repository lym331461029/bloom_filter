package main

import (
	"fmt"
	"log"
)
import "github.com/gomodule/redigo/redis"

func main() {
	conn, err := redis.Dial("tcp", "127.0.0.1:6380")
	if err != nil {
		log.Fatal(err.Error())
	}
	reply, err := conn.Do("new", "bloom_a", 100000, 0.000001)
	if err != nil {
		log.Fatal(err.Error())
	}

	fmt.Println(redis.String(reply, err))

	reply, err = conn.Do("add", "bloom_a", "test_data_1")
	if err != nil {
		log.Fatal(err.Error())
	}
	fmt.Println(redis.String(reply, err))

	reply, err = conn.Do("test", "bloom_a", "test_data_1")
	fmt.Println(redis.Int64(reply, err))

	reply, err = conn.Do("test", "bloom_a", "test_data_2")
	fmt.Println(redis.Int64(reply, err))

}
