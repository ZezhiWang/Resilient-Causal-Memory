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
	readBuf   map[int]ReadBufEntry
	writeBuf  map[int][]int
	localBuf  map[string]string
}

func (clt *Client) init() {
	// init vector timestamp with length group_size
	clt.vecClocks = make([]int, NumClient)
	for i:= 0; i < NumClient; i++ {
		clt.vecClocks[i] = 0
	}
	clt.counter = 0
	// init read buffer as counter(int) - (value, timestamp) tuple (ReadBufEntry) pairs
	clt.readBuf = make(map[int] ReadBufEntry)
	clt.writeBuf = make(map[int] []int)
	clt.localBuf = make(map[string] string)
}

func (clt *Client) read(key string) string {
	dealer := createDealerSocket()
	defer dealer.Close()
	msg := Message{Kind: READ, Key: key, Id: nodeId, Counter: clt.counter, Vec: clt.vecClocks}
	zmqBroadcast(&msg,dealer)
	fmt.Printf("Client %d broadcasted msg READ\n", nodeId)

	for i := 0; i < len(serverLists); i++{
		clt.recvRESP(dealer)
		if _,isIn := clt.readBuf[clt.counter]; isIn{
			break
		}
	}
	entry,_ := clt.readBuf[clt.counter]
	clt.counter += 1
	if smallerEqualExceptI(entry.vecClocks, clt.vecClocks, 999999) {
		val, isIn := clt.localBuf[key]
		if !isIn {
			panic("value is not in local buffer")
			return "Error"
		}
		return val
	} else {
		clt.mergeClock(entry.vecClocks)
		return entry.val
	}
}

func (clt *Client) write(key string, value string) {
	clt.vecClocks[nodeId] += 1
	dealer := createDealerSocket()
	defer dealer.Close()
	msg := Message{Kind: WRITE, Key: key, Val: value, Id: nodeId, Counter: clt.counter, Vec: clt.vecClocks}
	zmqBroadcast(&msg,dealer)
	fmt.Printf("Client %d broadcasted msg WRITE\n", nodeId)
	
	for i := 0; i < len(serverLists); i++{
		clt.recvACK(dealer)
		if _,isIn := clt.writeBuf[clt.counter]; isIn{
			break
		}
	}
	entry,_ := clt.writeBuf[clt.counter]
	clt.mergeClock(entry)
	clt.localBuf[key] = value
	clt.counter += 1
}

func (clt *Client) recvRESP(dealer *zmq.Socket){
	msgBytes,err := dealer.RecvBytes(0)
	if err != nil{
		fmt.Println("client.go line 84, dealer RecvBytes: {}", err)
	}
	msg := getMsgFromGob(msgBytes)
	if msg.Kind != RESP || msg.Counter != clt.counter {
		clt.recvRESP(dealer)
	} else{
		clt.readBuf[msg.Counter] = ReadBufEntry{val: msg.Val, vecClocks: msg.Vec}
		fmt.Println("RESP message received vec", msg.Vec)
	}
}

func (clt *Client) recvACK(dealer *zmq.Socket) {
	// counter int, vec []int
	msgBytes,err := dealer.RecvBytes(0)
	if err != nil{
		fmt.Println("client.go line 99, dealer RecvBytes: {}", err)
	}
	msg := getMsgFromGob(msgBytes)
	if msg.Kind != ACK || msg.Counter != clt.counter {
		clt.recvACK(dealer)
	} else{
		clt.writeBuf[msg.Counter] = msg.Vec
		fmt.Println("ACK message received vec", msg.Vec)
	}
}

func (clt *Client) mergeClock(vec []int) {
	if len(clt.vecClocks) != len(vec) {
		panic("vector clocks are of different lengths")
	}
	for i:=0; i<len(clt.vecClocks); i++ {
		if vec[i] > clt.vecClocks[i] {
			clt.vecClocks[i] = vec[i]
		}
	}
}