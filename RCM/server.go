package main

import (
	"time"
	"net"
	"fmt"
	"sync"
)

type WitnessEntry struct {
	id 		int
	counter int
}

type Server struct {
	m_data			map[int]string
	m_data_lock		sync.RWMutex
	vec_clock		[]int
	vec_clock_lock	sync.RWMutex
	queue			Queue
	witness 		map[WitnessEntry]int
	witness_lock	sync.RWMutex
}

func (svr *Server) init(group_size int) {
	// init data as key(int)-value(string) pair
	svr.m_data = make(map[int] string)
	svr.m_data_lock = sync.RWMutex{}
	// init vector timestamp with length group_size
	svr.vec_clock = make([]int, group_size)
	svr.vec_clock_lock = sync.RWMutex{}
	// set vector timestamp to zero
	for i:= 0; i < group_size; i++ {
		svr.vec_clock[i] = 0
	}
	// init queue
	svr.queue.Init()
	// init witness
	svr.witness = make(map[WitnessEntry] int)
	svr.witness_lock = sync.RWMutex{}
}

// Actions to take if server receives READ message
func (svr *Server) recvRead(key int, id int, counter int, vec_i []int){
	// wait until t_server is greater than t_i
	for !smallerEqualExceptI(vec_i, svr.vec_clock, 999999) {
		time.Sleep(time.Millisecond)
	}
	// send RESP message to client i
	svr.m_data_lock.RLock()
	msg := Message{Kind: RESP, Counter: counter, Val: svr.m_data[key], Vec: svr.vec_clock}
	svr.m_data_lock.RUnlock()
	send(&msg, mem_list[id])
}

// Actions to take if server receives WRITE message
func (svr *Server) recvWrite(key int, val string, id int, counter int, vec_i []int) {
	// broadcast UPDATE message
	msg := Message{Kind: UPDATE, Key: key, Val: val, Id: id, Counter: counter, Vec: vec_i}
	broadcast(&msg)
	// wait until t_server is greater than t_i
	for !smallerEqualExceptI(vec_i, svr.vec_clock, 999999) {
		time.Sleep(time.Millisecond)
	}
	// send ACK message to client i
	msg = Message{Kind: ACK, Counter: counter, Vec: svr.vec_clock}
	send(&msg, mem_list[id])
}

// Actions to take if server receives UPDATE message
func (svr *Server) recvUpdate(key int, val string, id int, counter int, vec_i []int) {
	entry := WitnessEntry{id: id, counter: counter}
	svr.witness_lock.RLock()
	num, isIn := svr.witness[entry]
	svr.witness_lock.RUnlock()
	svr.witness_lock.Lock()
	if isIn {
		svr.witness[entry] = num + 1
	} else {
		svr.witness[entry] = 1
	}
	svr.witness_lock.Unlock()
	svr.witness_lock.RLock()
	if svr.witness[entry] == 1 {
		msg := Message{Kind: UPDATE, Key: key, Val: val, Id: id, Counter: counter, Vec: vec_i}
		broadcast(&msg)
	}
	svr.witness_lock.RUnlock()
	svr.witness_lock.Lock()
	if svr.witness[entry] == F+1 {
		queue_entry := QueueEntry{Key: key, Val: val, Id: id, Vec: vec_i}
		svr.queue.Enqueue(queue_entry)
		svr.witness[entry] = F+2
	}
	svr.witness_lock.Unlock()
}

// Server listener
func (svr *Server) recv(){
	// resolve for udp address by membership list and id
	udpAddr,err1 := net.ResolveUDPAddr("udp4", mem_list[id])
	if err1 != nil {
		fmt.Println("address not found")
	}

	// create listner socket by address
	conn,err2 := net.ListenUDP("udp", udpAddr)
	if err2 != nil {
		fmt.Println("address can't listen")
	}
	defer conn.Close()

	for status {
		c := make(chan Message)

		go func() {
			//buffer size is 1024 bytes
			buf := make([]byte, 1024)
			num,_,err3 := conn.ReadFromUDP(buf)
			if err3 != nil {
				fmt.Println(err3)
			}
			//deserialize the received data and output to channel
			c <- getMsgFromGob(buf[:num])
		}()

		msg := <-c
		
		switch msg.Kind {
		case READ:
			go svr.recvRead(msg.Key, msg.Id, msg.Counter, msg.Vec)
		case WRITE:
			go svr.recvWrite(msg.Key, msg.Val, msg.Id, msg.Counter, msg.Vec)
		case UPDATE:
			go svr.recvUpdate(msg.Key, msg.Val, msg.Id, msg.Counter, msg.Vec)
		}
	}
}

// infinitely often update the local storage
func (svr *Server) update(){
	msg := svr.queue.Dequeue()
	if msg != nil {
		// wait until it's time to update
		fmt.Println(msg.Vec)
		fmt.Println(svr.vec_clock)
		for svr.vec_clock[msg.Id] != msg.Vec[msg.Id]-1 || !smallerEqualExceptI(msg.Vec, svr.vec_clock, msg.Id) {
			// time.Sleep(time.Millisecond)
			if svr.vec_clock[msg.Id] > msg.Vec[msg.Id]-1 {
				return
			}
		}
		// update timestamp and write to local memory
		svr.vec_clock_lock.Lock()
		svr.vec_clock[msg.Id] = msg.Vec[msg.Id]
		svr.vec_clock_lock.Unlock()
		svr.m_data_lock.Lock()
		svr.m_data[msg.Key] = msg.Val
		svr.m_data_lock.Unlock()
		fmt.Println("server ", id, " has m_data: ", svr.m_data)
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