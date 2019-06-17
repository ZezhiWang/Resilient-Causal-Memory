package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
)

var (
	addr  string
	//mutex  = &sync.Mutex{}
	// IP addresses of servers
	servers   = make(map[int]string)
	maxUsedTs = -1
)

// used to mark the phase
const GET = 0
const GETTS = 1
const STORE = 2
const ERR = 3
const ACK = 4

const READ_QUORUM = 3
const WRITE_QUORUM = 3
const F = 1

const DATASIZE = 1024

func main() {
	// init client id
	flag.StringVar(&addr, "clientIP", "128.52.179.161:8888", "input client IP")
	flag.Parse()

	// read config file
	config, err := os.Open("Config")
	if err != nil {
		fmt.Print(err)
		return
	}
	scanner := bufio.NewScanner(config)
	for scanner.Scan() {
		line := strings.Split(scanner.Text(), " ")
		id, err := strconv.Atoi(line[0])
		if err != nil {
			fmt.Println(err)
			return
		}
		servers[id] = line[1]
	}
	config.Close()

//	client()
	workload(10000)
}
	
