package main

import (
	"flag"
	"github.com/peterbourgon/diskv"
)

var (
	//mutex  = &sync.Mutex{}
	addr  string
	//mData  = make(map[int]TagVal)
	d = diskv.New(diskv.Options{
		BasePath:     "data",
	})
)

// used to mark the phase
const GET = 0
const GETTS = 1
const STORE = 2
const ERR = 3
const ACK = 4

func main(){
	// init storage IP
	flag.StringVar(&addr, "addr", "128.52.179.163", "input addr")
	flag.Parse()
	// create cassandra session
	server_task()
}
