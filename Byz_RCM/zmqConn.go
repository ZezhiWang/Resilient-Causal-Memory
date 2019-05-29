package main

import (
	"fmt"
 	zmq "github.com/pebbe/zmq4"
)

func createDealerSocket() *zmq.Socket {
	dealer,_ := zmq.NewSocket(zmq.DEALER)
	var addr string
	for _,server := range server_list {
		addr = "tcp://" + server
		dealer.Connect(addr)
	}
	return dealer
}

func createPublisherSocket(pubAddr string) *zmq.Socket {
	publisher,_ := zmq.NewSocket(zmq.PUB)
	publisher.Bind("tcp://" + pubAddr)
	return publisher
}


func createSubscriberSocket() *zmq.Socket {
	subscriber,_ := zmq.NewSocket(zmq.SUB)
	var addr string
	for _,server := range server_pub {
		addr = "tcp://" + server
		subscriber.Connect(addr)
	}
	subscriber.SetSubscribe(FILTER)
	return subscriber
}

func (svr *Server) publish(msg *Message) {
	b := getGobFromMsg(msg)
	// publisher.Send(FILTER, zmq.SNDMORE)
	svr.publisherLock.Lock()
	_, err := svr.publisher.SendBytes(b,0)
	svr.publisherLock.Unlock()
	if err != nil {
		fmt.Println("Error occurred at line 42 in file zmqConn.go", err)
	}
}

// broadcast to all
func zmqBroadcast(msg *Message, dealer *zmq.Socket){
	//use gob to serialized data before sending
	b := getGobFromMsg(msg)
	for i := 0; i < len(server_list); i++ {
		_, err := dealer.SendBytes(b,0)
		if (err != nil) {
			fmt.Println("Error occurred when dealer sending msg, ", err)
		}
	}
}