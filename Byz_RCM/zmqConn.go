package main

import (
	"fmt"
 	zmq "github.com/pebbe/zmq4"
)

func createDealerSocket() *zmq.Socket {
	dealer,_ := zmq.NewSocket(zmq.DEALER)
	var addr string
	for _,server := range serverLists {
		addr = "tcp://" + server
		if err := dealer.Connect(addr); err != nil{
			fmt.Println("err connecting", addr)
		}
	}
	return dealer
}

func createPublisherSocket(pubAddr string) *zmq.Socket {
	publisher,_ := zmq.NewSocket(zmq.PUB)
	if err := publisher.Bind("tcp://" + pubAddr); err != nil{
		fmt.Println("err binding pub", pubAddr)
	}
	return publisher
}


func createSubscriberSocket() *zmq.Socket {
	subscriber,_ := zmq.NewSocket(zmq.SUB)
	var addr string
	for _,server := range serverPubs {
		addr = "tcp://" + server
		if err := subscriber.Connect(addr); err != nil{
			fmt.Println("err subscribing", addr)
		}
	}
	if err := subscriber.SetSubscribe(FILTER); err != nil{
		fmt.Println("err setting filter")
	}
	return subscriber
}

func (svr *Server) publish(msg *Message) {
	b := getGobFromMsg(msg)
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
	for i := 0; i < len(serverLists); i++ {
		if _, err := dealer.SendBytes(b,0); err != nil {
			fmt.Println("Error occurred when dealer sending msg, ", err)
		}
	}
}