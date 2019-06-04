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
	nodeId   int
	nodeType string
	// mutex = new(sync.mutex)
	serverLists = make(map[int]string)
	serverPubs  = make(map[int]string)
	//status      bool
)

func main() {
	flag.StringVar(&nodeType, "type", "server", "specify the node type")

	// up and running
	//status = true

	// get node id
	flag.IntVar(&nodeId, "id", 0, "specify the node id")
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
		serverLists[id] = line[1]
		serverPubs[id] = line[2]
	}
	if err := config.Close(); err != nil{
		fmt.Println("err closing config")
	}

	switch nodeType {
	case "server":
		var node Server

		node.init(serverPubs[nodeId])
		go node.serverTask(serverLists[nodeId])

		done := make(chan bool)
		<- done
	case "client":
		var node Client
		node.init()
		// node.userInput()
		node.workload(10000)
	}
}