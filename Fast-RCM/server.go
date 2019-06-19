package main

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"sync"
)

type WitnessEntry struct {
	id 		int
	counter int
}

type Server struct {
	vecClocks     []int
	vecClockLock  sync.RWMutex
	vecClockCond  *sync.Cond
	queue         Queue
	witness       map[WitnessEntry]map[int]bool
	hasSent       map[WitnessEntry]bool
	hasSentLock   sync.Mutex
	publisherLock sync.Mutex
	publisher     *zmq.Socket
	subscriber    *zmq.Socket
}

func (svr *Server) init(pubPort string) {
	// init vector timestamp with length group_size
	svr.vecClocks = make([]int, NumClient)
	svr.vecClockLock = sync.RWMutex{}
	svr.vecClockCond = sync.NewCond(&svr.vecClockLock)
	// set vector timestamp to zero
	for i := 0; i < NumClient; i++ {
		svr.vecClocks[i] = 0
	}
	// init queue
	svr.queue.Init()
	// init witness
	svr.witness = make(map[WitnessEntry]map[int]bool)
	svr.hasSent = make(map[WitnessEntry]bool)
	svr.hasSentLock = sync.Mutex{}
	svr.publisher = createPublisherSocket(pubPort)
	svr.subscriber = createSubscriberSocket()
	go svr.subscribe()
}

func (svr *Server) recvRead(key string, id int, counter int, vecI []int) *Message{
	msg := Message{Kind: RESP, Counter: counter, Val: d.ReadString(key), Vec: svr.vecClocks}
	return &msg
}

func (svr *Server) recvWrite(key string, val string, id int, counter int, vecI []int) *Message{
	msg := Message{Kind: UPDATE, Key: key, Val: val, Id: id, Counter: counter, Vec: vecI, Sender: nodeId}
	entry := WitnessEntry{id: id, counter: counter}
	svr.hasSentLock.Lock()
	if _,isIn := svr.hasSent[entry]; !isIn {
		svr.publish(&msg)
		svr.hasSent[entry] = true
		// fmt.Printf("Server %d published msg UPDATE in response to WRITE from client %d\n", nodeId, id)
	}
	svr.hasSentLock.Unlock()

	replyMsg := Message{Kind: ACK, Counter: counter, Vec: svr.vecClocks}
	return &replyMsg
}

// Actions to take if server receives UPDATE message
func (svr *Server) recvUpdate(key string, val string, id int, counter int, vecI []int, senderId int) {
	entry := WitnessEntry{id: id, counter: counter}
	if _,isIn := svr.witness[entry]; isIn {
		if _,hasReceived := svr.witness[entry][senderId]; !hasReceived {
			svr.witness[entry][senderId] = true

			// Publish UPDATE
			svr.hasSentLock.Lock()
			if _,isIn := svr.hasSent[entry]; !isIn {
				msg := Message{Kind: UPDATE, Key: key, Val: val, Id: id, Counter: counter, Vec: vecI, Sender: nodeId}
				svr.publish(&msg)
				svr.hasSent[entry] = true
				// fmt.Printf("Server %d published msg UPDATE in response to UPDATE from server %d\n", nodeId, senderId)
			}
			svr.hasSentLock.Unlock()

			if len(svr.witness[entry]) == F+1 {
				queueEntry := QueueEntry{Key: key, Val: val, Id: id, Vec: vecI}
				svr.queue.Enqueue(queueEntry)
				go svr.update()
				// fmt.Println("server enqueues entry: ", queueEntry)
			}
		}
	} else {
		svr.witness[entry] = make(map[int]bool)
		svr.witness[entry][senderId] = true

		// Publish UPDATE
		svr.hasSentLock.Lock()
		if _,isIn := svr.hasSent[entry]; !isIn {
			msg := Message{Kind: UPDATE, Key: key, Val: val, Id: id, Counter: counter, Vec: vecI, Sender: nodeId}
			svr.publish(&msg)
			svr.hasSent[entry] = true
			// fmt.Printf("Server %d published msg UPDATE in response to UPDATE from server %d\n", nodeId, senderId)
		}
		svr.hasSentLock.Unlock()

		if len(svr.witness[entry]) == F+1 {
			queueEntry := QueueEntry{Key: key, Val: val, Id: id, Vec: vecI}
			svr.queue.Enqueue(queueEntry)
			go svr.update()
			// fmt.Println("server enqueues entry: ", queueEntry)
		}
	}
}

// infinitely often update the local storage
func (svr *Server) update(){
	msg := svr.queue.Dequeue()
	if msg != nil {
		// fmt.Println("server receives msg with vecClocks: ", msg.Vec)
		// fmt.Println("server has vecClocks: ", svr.vecClocks)
		svr.vecClockCond.L.Lock()
		for svr.vecClocks[msg.Id] != msg.Vec[msg.Id]-1 || !smallerEqualExceptI(msg.Vec, svr.vecClocks, msg.Id) {
			if svr.vecClocks[msg.Id] > msg.Vec[msg.Id]-1 {
				return
			}
			svr.vecClockCond.Wait()
		}
		// update timestamp and write to local memory
		svr.vecClocks[msg.Id] = msg.Vec[msg.Id]
		// fmt.Println("server increments vecClocks: ", svr.vecClocks)
		svr.vecClockCond.Broadcast()
		svr.vecClockCond.L.Unlock()
		if err := d.WriteString(msg.Key,msg.Val); err != nil {
			fmt.Println(err)
		}
	}
}

// helper function that return true if elements of vec1 are smaller than those of vec2 except i-th element; false otherwise
func smallerEqualExceptI(vec1 []int, vec2 []int, i int) bool {
	if len(vec1) != len(vec2) {
		panic("vector clocks are of different lengths")
	}
	for index:=0; index<len(vec1); index++ {
		if index == i {
			continue
		}
		if vec1[index] > vec2[index] {
			return false
		}
	}
	return true
}