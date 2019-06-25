package main

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"sync"
)

type WitnessEntry struct {
	id      int
	counter int
}

type Server struct {
	vecClock      [NumClient]int
	vecClockLock  sync.Mutex
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
	svr.vecClock = [NumClient]int{0}
	svr.vecClockLock = sync.Mutex{}
	svr.vecClockCond = sync.NewCond(&svr.vecClockLock)
	// set vector timestamp to zero
	for i := 0; i < NumClient; i++ {
		svr.vecClock[i] = 0
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

// Actions to take if server receives READ message
func (svr *Server) recvRead(key string, id int, counter int, vecI [NumClient]int) *Message {
	// wait until M[k].t is greater than t_i
	//svr.waitUntilServerClockGreaterExceptI(key, vecI, 999999)
	svr.waitUntilServerClockGreater(vecI)

	// send RESP message to client i
	ety := readFromDisk(key)
	msg := Message{Kind: RESP, Counter: counter, Val: ety.Val, Ts: ety.Ts, Sender: nodeId}
	return &msg
}

// Actions to take if server receives WRITE message
func (svr *Server) recvWrite(key string, val string, id int, counter int, vecI [NumClient]int) *Message{
	//fmt.Println(nodeId, "recv write", vecI, "from client", id)
	// broadcast UPDATE message
	msg := Message{Kind: UPDATE, Key: key, Val: val, Id: id, Counter: counter, Ts: vecI, Sender: nodeId}

	entry := WitnessEntry{id: id, counter: counter}

	svr.hasSentLock.Lock()
	if _,isIn := svr.hasSent[entry]; !isIn {
		svr.publish(&msg)
		svr.hasSent[entry] = true
		//fmt.Println(nodeId, "sent update of counter ", counter, "in recv write")
	}
	svr.hasSentLock.Unlock()

	// wait until M[k].t is greater than t_i
	//svr.waitUntilServerClockGreaterExceptI(key, vecI, 999999)
	svr.waitUntilServerClockGreater(vecI)

	// send ACK message to client i
	msg = Message{Kind: ACK, Counter: counter, Sender: nodeId}
	//fmt.Println(nodeId, "reply", msg)
	return &msg
}

// Actions to take if server receives CHECK message
func (svr *Server) recvCheck(key string, val string, counter int, vecI [NumClient]int) *Message{
	// hist := histFromDisk(key)
	msg := Message{Kind: ERROR, Val: val, Ts: svr.vecClock, Counter: counter, Sender: nodeId}
	//histLock.Lock()
	hist,err := histFromDisk(key)
	//hist,isIn := h[key]
	//histLock.Unlock()
	if err == nil {
		for _, ety := range hist {
			if isEqual(ety, TagVal{Val: val, Ts: vecI}) {
				msg.Kind = MATCH
				break
			}
		}
	}
	return &msg
}

// Actions to take if server receives UPDATE message
func (svr *Server) recvUpdate(key string, val string, id int, counter int, vecI [NumClient]int, senderId int) {
	//fmt.Println(nodeId, "recv update", vecI, "from svr", senderId)
	entry := WitnessEntry{id: id, counter: counter}

	// if this is first UPDATE msg from sender
	if _, isIn := svr.witness[entry]; !isIn {
		svr.witness[entry] = make(map[int]bool)
	}
	svr.witness[entry][senderId] = true

	// if received F + 1 unique UPDATE msg
	if len(svr.witness[entry]) == F+1 {
		// Publish UPDATE
		svr.hasSentLock.Lock()
		if _, isIn := svr.hasSent[entry]; !isIn {
			msg := Message{Kind: UPDATE, Key: key, Val: val, Id: id, Counter: counter, Ts: vecI, Sender: nodeId}
			svr.publish(&msg)
			svr.hasSent[entry] = true
			//fmt.Println(nodeId, "sent update of counter ", counter, "in recv update")
		}
		svr.hasSentLock.Unlock()
	}

	// if received N - F unique UPDATE msg
	if len(svr.witness[entry]) == len(serverLists) - F {
		queueEntry := QueueEntry{Key: key, Val: val, Id: id, Vec: vecI}
		svr.queue.Enqueue(queueEntry)
		go svr.update()
	}
}

// infinitely often update the local storage
func (svr *Server) update() {
	//fmt.Println(nodeId, "in update")
	ety := svr.queue.Dequeue()
	if ety != nil {
		svr.vecClockCond.L.Lock()
		for svr.vecClock[ety.Id] != ety.Vec[ety.Id]-1 || !smallerEqualExceptI(ety.Vec, svr.vecClock, ety.Id) {
			if svr.vecClock[ety.Id] > ety.Vec[ety.Id]-1 {
				//fmt.Println(nodeId,"end update")
				return
			}
			svr.vecClockCond.Wait()
		}
		// update timestamp and write to local memory
		svr.vecClock[ety.Id] += 1
		storeToDisk(ety.Key,&TagVal{Val: ety.Val, Ts:ety.Vec})
		//fmt.Println(nodeId, "update to disk")

		svr.vecClockCond.Broadcast()
		svr.vecClockCond.L.Unlock()
	}
}

// helper function that return true if elements of vec1 are smaller than those of vec2 except i-th element; false otherwise
func smallerEqualExceptI(vec1 [NumClient]int, vec2 [NumClient]int, i int) bool {
	//if len(vec1) != len(vec2) {
	//	panic("vector clocks are of different lengths")
	//}
	for index := 0; index < NumClient; index++ {
		if index == i {
			continue
		}
		if vec1[index] > vec2[index] {
			return false
		}
	}
	return true
}
//
//func (svr *Server) waitUntilServerClockGreaterExceptI(key string, vec [NumClient]int, i int) {
//	svr.vecClockCond.L.Lock()
//	ety := readFromDisk(key)
//	for !smallerEqualExceptI(vec, ety.Ts, i) {
//		fmt.Println("WRITE", nodeId, "client:", vec, "disk:", ety.Ts)
//		svr.vecClockCond.Wait()
//		ety = readFromDisk(key)
//	}
//	svr.vecClockCond.L.Unlock()
//}

func (svr *Server) waitUntilServerClockGreater(vec [NumClient]int) {
	svr.vecClockCond.L.Lock()
	for !smallerEqualExceptI(vec, svr.vecClock, 999999) {
		fmt.Println("READ", nodeId, "client:", vec, "disk:", svr.vecClock)
		svr.vecClockCond.Wait()
	}
	svr.vecClockCond.L.Unlock()
}
