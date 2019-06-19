package main

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
)

type ReadBufEntry struct {
	val       string
	vecClocks []int
}

type Client struct {
	vecClocks []int
	counter   int
	writerTs  map[int]map[int][]int
	readBuf   map[int]ReadBufEntry
}

func (clt *Client) init() {
	// init vector timestamp with length group_size
	clt.vecClocks = make([]int, NumClient)
	// set vector timestamp to zero
	for i := 0; i < NumClient; i++ {
		clt.vecClocks[i] = 0
	}
	clt.counter = 0
	// init writerTs as counter(int) - timestamp([]int) pairs
	clt.writerTs = make(map[int]map[int][]int)
	// init read buffer as counter(int) - (value, timestamp) tuple (ReadBufEntry) pairs
	clt.readBuf = make(map[int]ReadBufEntry)
}

func (clt *Client) read(key string) string {
	dealer := createDealerSocket()
	defer dealer.Close()
	msg := Message{Kind: READ, Key: key, Id: nodeId, Counter: clt.counter, Vec: clt.vecClocks}
	zmqBroadcast(&msg, dealer)
	fmt.Printf("Client %d broadcasted msg READ\n", nodeId)

	for i:=0; i < len(serverLists); i++{
		clt.recvRESP(dealer)
		_,isIn := clt.readBuf[clt.counter]
		if isIn{
			break
		}
	}
	entry,_ := clt.readBuf[clt.counter]
	clt.mergeClock(entry.vecClocks)
	clt.counter += 1
	return entry.val
}

func (clt *Client) write(key string, value string) {
	var numAck int
	dealer := createDealerSocket()
	defer dealer.Close()
	clt.vecClocks[nodeId] += 1
	msg := Message{Kind: WRITE, Key: key, Val: value, Id: nodeId, Counter: clt.counter, Vec: clt.vecClocks}
	zmqBroadcast(&msg, dealer)
	fmt.Printf("Client %d broadcasted msg WRITE\n", nodeId)

	for i:=0; i < len(serverLists); i++{
		clt.recvACK(dealer)
		numAck = len(clt.writerTs[clt.counter])
		if numAck > F {
			break
		}
	}
	vecSets := clt.writerTs[clt.counter]
	// merge all elements of writerTs[counter] with local vector clock
	for _, vec := range vecSets {
		clt.mergeClock(vec)
	}
	clt.counter += 1
}

// Actions to take if receive RESP message
func (clt *Client) recvRESP(dealer *zmq.Socket) {
	msgBytes, err := dealer.RecvBytes(0)
	if err != nil {
		fmt.Println("Error occurred when client receiving RESP, err msg: ", err)
		fmt.Println(dealer.String())
	}
	msg := getMsgFromGob(msgBytes)
	if msg.Kind != RESP || msg.Counter != clt.counter {
		clt.recvRESP(dealer)
	} else {
		entry := ReadBufEntry{val: msg.Val, vecClocks: msg.Vec}
		clt.readBuf[msg.Counter] = entry
	}
	return
}

// Actions to take if receive ACK message
func (clt *Client) recvACK(dealer *zmq.Socket) {
	msgBytes, err := dealer.RecvBytes(0)
	if err != nil {
		fmt.Println("Error occurred when client receiving ACK, err msg: ", err)
		fmt.Println(dealer.String())
	}
	msg := getMsgFromGob(msgBytes)
	if msg.Kind != ACK || msg.Counter != clt.counter {
		clt.recvACK(dealer)
	} else{
		if _, isIn := clt.writerTs[msg.Counter]; !isIn {
			clt.writerTs[msg.Counter] = make(map[int][]int)
		}
		fmt.Println("ACK message received vec", msg.Vec)
		clt.writerTs[msg.Counter][len(clt.writerTs[msg.Counter])] = msg.Vec
	}
	return
}

// helper function that merges a vector clock with client's own vector clock
func (clt *Client) mergeClock(vec []int) {
	if len(clt.vecClocks) != len(vec) {
		fmt.Println(clt.vecClocks)
		fmt.Println(vec)
		panic("vector clocks are of different lengths")
	}

	for i := 0; i < len(clt.vecClocks); i++ {
		if vec[i] > clt.vecClocks[i] {
			clt.vecClocks[i] = vec[i]
		}
	}
}
