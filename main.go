package main

import (
	"log"
	"strconv"
	"strings"
	"sync"

	"github.com/tidwall/redcon"
	"github.com/willf/bloom"
)

var addr = ":6380"

func main() {
	go log.Printf("started server at %s", addr)

	err := redcon.ListenAndServe(addr,
		func(conn redcon.Conn, cmd redcon.Command) {
			switch strings.ToLower(string(cmd.Args[0])) {
			default:
				conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
			case "ping":
				conn.WriteString("PONG")
			case "quit":
				conn.WriteString("OK")
				conn.Close()
			case "get":


			case "new":
				if len(cmd.Args) != 4 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}
				filterName := string(cmd.Args[1])
				elementNumber, err := strconv.ParseUint(string(cmd.Args[2]), 10, 64)
				if err != nil {
					conn.WriteError("ERR invalid arguments")
					return
				}

				rate, err := strconv.ParseFloat(string(cmd.Args[3]), 64)
				if err != nil {
					conn.WriteError("ERR invalid arguments")
					return
				}
				filter := bloom.NewWithEstimates(uint(elementNumber), rate)
				cache.Store(filterName, filter)
				conn.WriteString("OK")
			case "add":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}

				obj, ok := cache.Load(string(cmd.Args[1]))
				if ok {
					filter := obj.(*bloom.BloomFilter)
					filter.Add(cmd.Args[2])
					conn.WriteString("OK")
				} else {
					conn.WriteError("filter not found")
				}
			case "test":
				if len(cmd.Args) != 3 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}

				obj, ok := cache.Load(string(cmd.Args[1]))
				if ok {
					filter := obj.(*bloom.BloomFilter)
					exist := filter.Test(cmd.Args[2])
					if exist {
						conn.WriteInt(1)
					} else {
						conn.WriteInt(0)
					}
				} else {
					conn.WriteError("filter not found")
				}
			case "del":
				if len(cmd.Args) != 2 {
					conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
					return
				}

				_, b := cache.LoadAndDelete(string(cmd.Args[1]))
				if b {
					conn.WriteInt(1)
				} else {
					conn.WriteInt(0)
				}
			}
		},
		func(conn redcon.Conn) bool {
			// Use this function to accept or deny the connection.
			// log.Printf("accept: %s", conn.RemoteAddr())
			return true
		},
		func(conn redcon.Conn, err error) {
			// This is called when the connection has been closed
			// log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
		},
	)
	if err != nil {
		log.Fatal(err)
	}
}

//var cache = map[string]*bloom.BloomFilter{}

var cache = sync.Map{}
