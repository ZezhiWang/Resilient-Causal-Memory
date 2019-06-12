package main

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
)

type ReadBufEntry struct {
	val      	string
	vec		 	[NUM_CLIENT]int
}

type Client struct {
	vecClock [NUM_CLIENT]int
	counter  int
	writeBuf map[int]map[int]bool
	readBuf  map[int]map[ReadBufEntry]map[int]bool
	hasResp  map[int]map[int]bool
	tvChan   chan TagVal
}

func (clt *Client) init() {
	// init vector timestamp with length group_size
	clt.vecClock = [NUM_CLIENT]int{}
	// set vector timestamp to zero
	for i := 0; i < NUM_CLIENT; i++ {
		clt.vecClock[i] = 0
	}
	clt.counter = 0
	// init writeBuf as counter(int) - timestamp([]int) pairs
	clt.writeBuf = make(map[int]map[int]bool)
	// init read buffer as counter(int) - (value, timestamp) tuple (ReadBufEntry) pairs
	clt.readBuf = make(map[int]map[ReadBufEntry]map[int]bool)
	// init has response
	clt.hasResp = make(map[int]map[int]bool)

	clt.tvChan = make(chan TagVal, 1)
}

func (clt *Client) read(key string) string {
	var res TagVal
	var shouldBreak bool
	dealer := createDealerSocket()
	defer dealer.Close()
	msg := Message{Kind: READ, Key: key, Id: nodeId, Counter: clt.counter, Ts: clt.vecClock}
	zmqBroadcast(&msg, dealer)
	fmt.Printf("Client %d broadcasted msg READ\n", nodeId)

	// at most N RESP and N * F + 1 MATCH
	for i:=0; i < len(serverLists) * (F + 1) + 2; i++ {
		res,shouldBreak = clt.recvRESP(dealer)
		if shouldBreak{
			break
		}
	}

	// merge vector clock
	clt.mergeClock(res.Ts)

	delete(clt.hasResp,clt.counter)
	delete(clt.readBuf,clt.counter)

	clt.counter += 1
	return res.Val
}

func (clt *Client) write(key string, value string) {
	dealer := createDealerSocket()
	defer dealer.Close()
	clt.vecClock[nodeId] += 1
	msg := Message{Kind: WRITE, Key: key, Val: value, Id: nodeId, Counter: clt.counter, Ts: clt.vecClock}
	zmqBroadcast(&msg, dealer)
	fmt.Printf("Client %d broadcasted msg WRITE\n", nodeId)

	for i:=0; i < len(serverLists); i++{
		clt.recvACK(dealer)
		if acks,isIn := clt.writeBuf[clt.counter]; isIn && len(acks) > F {
			break
		}
	}

	delete(clt.writeBuf,clt.counter)
	clt.counter += 1
}

// Actions to take if receive RESP message
func (clt *Client) recvRESP(dealer *zmq.Socket) (TagVal,bool) {
	msgBytes, err := dealer.RecvBytes(0)
	if err != nil {
		fmt.Println("Error occurred when client receiving ACK, err msg: ", err)
		fmt.Println(dealer.String())
	}
	msg := getMsgFromGob(msgBytes)

	if msg.Kind == MATCH{
		if _,isIn := clt.readBuf[msg.Counter]; !isIn {
			clt.readBuf[msg.Counter] = make(map[ReadBufEntry]map[int]bool)
		}

		readEty := ReadBufEntry{val: msg.Val, vec: msg.Vec}
		if _,isIn := clt.readBuf[msg.Counter][readEty]; !isIn{
			clt.readBuf[msg.Counter][readEty] = make(map[int]bool)
		}
		clt.readBuf[msg.Counter][readEty][msg.Sender] = true

		if len(clt.readBuf[msg.Counter][readEty]) > F {
			return TagVal{Val: msg.Val, Ts: msg.Ts},true
		}
	}

	if msg.Kind == RESP && msg.Counter == clt.counter {
		if _,isIn := clt.hasResp[msg.Counter]; !isIn {
			clt.hasResp[msg.Counter] = make(map[int]bool)
		}

		if _,isIn := clt.hasResp[msg.Counter][msg.Sender]; !isIn {
			if !smallerEqualExceptI(msg.Ts,clt.vecClock,99999){
				msg.Kind = CHECK
				zmqBroadcast(&msg,dealer)
			}
		}
		clt.hasResp[msg.Counter][msg.Sender] = true
	}

	return TagVal{[NUM_CLIENT]int{},""},false
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
	} else {
		if _, isIn := clt.writeBuf[msg.Counter]; !isIn {
			clt.writeBuf[msg.Counter] = make(map[int]bool)
		}
		clt.writeBuf[msg.Counter][msg.Sender] = true
		//fmt.Println("client", clt.writeBuf[msg.Counter])
	}
}

// helper function that merges a vector clock with client's own vector clock
func (clt *Client) mergeClock(vec [NUM_CLIENT]int) {
	//if len(clt.vecClock) != len(vec) {
	//	fmt.Println(clt.vecClock)
	//	fmt.Println(vec)
	//	panic("vector clocks are of different lengths")
	//}
	for i := 0; i < NUM_CLIENT; i++ {
		if vec[i] > clt.vecClock[i] {
			clt.vecClock[i] = vec[i]
		}
	}
}