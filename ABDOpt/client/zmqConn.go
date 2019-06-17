package main 

import (
	zmq "github.com/pebbe/zmq4"
)

// init dealer socket
func createDealerSocket() *zmq.Socket {
	dealer,_ := zmq.NewSocket(zmq.DEALER)
	var addr string
	for _,server := range servers {
		addr = "tcp://" + server
		dealer.Connect(addr)
	}
	return dealer
}

// broadcast to all server
func sendToServer(msg Message, dealer *zmq.Socket) {
	msgToSend := getGobFromMsg(msg)
	for i := 0 ; i < len(servers); i++ {
		dealer.SendBytes(msgToSend.Bytes(), 0)
	}
}

// recv msg with data
func recvData(dealer *zmq.Socket) TagVal {
	msgBytes,_ := dealer.RecvBytes(0)
	msg := getMsgFromGob(msgBytes)
	if msg.OpType != GET {
		return recvData(dealer)
	}
	return msg.Tv
}

// recv msg without data
func recvAck(dealer *zmq.Socket) {
	msgBytes,_ := dealer.RecvBytes(0)
	msg := getMsgFromGob(msgBytes)
	if msg.OpType != SET {
		recvAck(dealer)
	}
}
