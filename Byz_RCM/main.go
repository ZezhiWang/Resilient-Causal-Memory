package main

import (
	"flag"
	"fmt"
	"os"
	// "time"
	// "sync"
	"bufio"
	"strconv"
	"strings"
)

var (
	node_id     int
	node_type 	string
	// mutex = new(sync.mutex)
	server_list = make(map[int]string)
	server_pub = make(map[int]string)
	status   	bool
)

func main() {
	//node_type = os.Getenv("type")
	flag.StringVar(&node_type, "type", "server", "specify the node type")

	// up and running
	status = true

	// get node id
	// id,_ = strconv.Atoi(os.Getenv("id"))
	flag.IntVar(&node_id, "id", 0, "specify the node id")
	flag.Parse()
	// read config file
	config, err := os.Open("config.txt")
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
		server_list[id] = line[1]
		server_pub[id] = line[2]
	}
	config.Close()

	switch node_type {
	case "server":
		var node Server

		node.init(server_pub[node_id])
		go node.serverTask(server_list[node_id])

		done := make(chan bool)
		<- done
	case "client":
		var node Client
		node.init()
		//go node.recv()
		// node.userInput()
		node.workload(10000)
	}
}