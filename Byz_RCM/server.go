package main

import (
	zmq "github.com/pebbe/zmq4"
	"sync"
)

type WitnessEntry struct {
	key 	string
	val 	string
	id      int
	counter int
}

type Server struct {
	vecClock      [NUM_CLIENT]int
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
	svr.vecClock = [NUM_CLIENT]int{0}
	svr.vecClockLock = sync.Mutex{}
	svr.vecClockCond = sync.NewCond(&svr.vecClockLock)
	// set vector timestamp to zero
	for i := 0; i < NUM_CLIENT; i++ {
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
func (svr *Server) recvRead(key string, id int, counter int, vecI [NUM_CLIENT]int) *Message {
	// wait until M[k].t is greater than t_i
	svr.waitUntilServerClockGreaterExceptI(key, vecI, 999999)

	// send RESP message to client i
	ety := readFromDisk(key)
	msg := Message{Kind: RESP, Counter: counter, Val: ety.val, Vec: ety.ts}
	return &msg
}

// Actions to take if server receives WRITE message
func (svr *Server) recvWrite(key string, val string, id int, counter int, vecI [NUM_CLIENT]int) *Message{
	// broadcast UPDATE message
	msg := Message{Kind: UPDATE, Key: key, Val: val, Id: id, Counter: counter, Vec: vecI, Sender: nodeId}

	entry := WitnessEntry{id: id, counter: counter}

	svr.hasSentLock.Lock()
	if _,isIn := svr.hasSent[entry]; !isIn {
		svr.publish(&msg)
		svr.hasSent[entry] = true
	}
	svr.hasSentLock.Unlock()

	// wait until M[k].t is greater than t_i
	svr.waitUntilServerClockGreaterExceptI(key, vecI, 999999)

	// send ACK message to client i
	msg = Message{Kind: ACK, Counter: counter, Vec: [NUM_CLIENT]int{}}
	return &msg
}

// Actions to take if server receives CHECK message
func (svr *Server) recvCheck(key string, val string, id int, counter int, vecI [NUM_CLIENT]int) *Message{
	hist := histFromDisk(key)
	msg := Message{Kind: ERROR, Key: key, Val: val, Id: id, Counter: counter, Vec: vecI}
	for _,ety := range hist{
		if isEqual(ety,TagVal{val:val,ts: vecI}){
			msg.Kind = MATCH
			break
		}
	}
	return &msg
}

// Actions to take if server receives UPDATE message
func (svr *Server) recvUpdate(key string, val string, id int, counter int, vecI [NUM_CLIENT]int, senderId int) {
	entry := WitnessEntry{key: key, val: val, id: id, counter: counter}

	// if this is first UPDATE msg from sender
	if _, isIn := svr.witness[entry]; isIn {
		if _, hasReceived := svr.witness[entry][senderId]; !hasReceived {
			svr.witness[entry][senderId] = true
		}
	} else {
		svr.witness[entry] = make(map[int]bool)
		svr.witness[entry][senderId] = true
	}

	// if received F + 1 unique UPDATE msg
	if len(svr.witness[entry]) == F+1 {
		// Publish UPDATE
		svr.hasSentLock.Lock()
		if _, isIn := svr.hasSent[entry]; !isIn {
			msg := Message{Kind: UPDATE, Key: key, Val: val, Id: id, Counter: counter, Vec: vecI, Sender: nodeId}
			svr.publish(&msg)
			svr.hasSent[entry] = true
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
	msg := svr.queue.Dequeue()
	if msg != nil {
		svr.vecClockCond.L.Lock()
		for svr.vecClock[msg.Id] != msg.Vec[msg.Id]-1 || !smallerEqualExceptI(msg.Vec, svr.vecClock, msg.Id) {
			if svr.vecClock[msg.Id] > msg.Vec[msg.Id]-1 {
				return
			}
			svr.vecClockCond.Wait()
		}
		// update timestamp and write to local memory
		svr.vecClock[msg.Id] = msg.Vec[msg.Id]
		mEty := readFromDisk(msg.Key)
		histAppend(msg.Key,TagVal{val:mEty.val,ts:mEty.ts})
		storeToDisk(msg.Key,&TagVal{val:msg.Val,ts:svr.vecClock})

		svr.vecClockCond.Broadcast()
		svr.vecClockCond.L.Unlock()
	}
}

// helper function that return true if elements of vec1 are smaller than those of vec2 except i-th element; false otherwise
func smallerEqualExceptI(vec1 [NUM_CLIENT]int, vec2 [NUM_CLIENT]int, i int) bool {
	//if len(vec1) != len(vec2) {
	//	panic("vector clocks are of different lengths")
	//}
	for index := 0; index < NUM_CLIENT; index++ {
		if index == i {
			continue
		}
		if vec1[index] > vec2[index] {
			return false
		}
	}
	return true
}

func (svr *Server) waitUntilServerClockGreaterExceptI(key string, vec [NUM_CLIENT]int, i int) {
	svr.vecClockCond.L.Lock()
	ety := readFromDisk(key)
	for !smallerEqualExceptI(vec, ety.ts, i) {
		svr.vecClockCond.Wait()
	}
	svr.vecClockCond.L.Unlock()
}
