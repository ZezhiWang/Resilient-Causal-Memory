package main

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"log"
)

func (svr *Server) serverTask(svrAddr string) {
	// Set the ZMQ sockets
	frontend,_ := zmq.NewSocket(zmq.ROUTER)
	defer fmt.Println("frontend socket closed")
	defer frontend.Close()
	frontend.Bind("tcp://" + svrAddr)

	//  Backend socket talks to workers over inproc
	backend, _ := zmq.NewSocket(zmq.DEALER)
	defer fmt.Println("backend socket closed")
	defer backend.Close()
	backend.Bind("inproc://backend")

	go svr.serverWorker()

	//  Connect backend to frontend via a proxy
	err := zmq.Proxy(frontend, backend, nil)
	log.Fatal("Proxy interrupted:", err)
}

func (svr *Server) serverWorker() {
	worker, _ := zmq.NewSocket(zmq.DEALER)
	defer fmt.Println("worker socket closed")
	defer worker.Close()
	worker.Connect("inproc://backend")
	msgReply := make([][]byte, 2)

	for i := 0; i < len(msgReply); i++ {
		msgReply[i] = make([]byte, 0) // the first frame  specifies the identity of the sender, the second specifies the content
	}

	for {
		msg,err := worker.RecvMessageBytes(0)
		if err != nil {
			fmt.Println(err)
		}
		// decode message
		message := getMsgFromGob(msg[1])
		if message.Kind == ERROR{
			continue
		}
		msgReply[0] = msg[0]

		// create response message
		tmpMsg := svr.createRep(message)
		// encode message
		tmpGob := getGobFromMsg(tmpMsg)
		msgReply[1] = tmpGob

		numBytes, err := worker.SendMessage(msgReply)
		if err != nil {
			fmt.Println("Error occurred when server worker sending reply, # of bytes sent is ", numBytes)
		}
	}
}

// create response message
func (svr *Server) createRep(input Message) *Message {
	var output *Message
	switch input.Kind {
		case READ:
			// fmt.Println("server receives READ message with vec_clock", msg.Vec)
			output = svr.recvRead(input.Key, input.Id, input.Counter, input.Vec)
		case WRITE:
			// fmt.Println("server receives WRITE message with vec_clock", msg.Vec)
			output = svr.recvWrite(input.Key, input.Val, input.Id, input.Counter, input.Vec)
		case CHECK:
			output = svr.recvCheck(input.Key,input.Val,input.Id,input.Counter,input.Vec)
	}
	return output
}

func (svr *Server) subscribe(){
	for{
		// get bytes
		b,_ := svr.subscriber.RecvMessageBytes(0)
		msg := getMsgFromGob(b[0])
		if msg.Kind != UPDATE{
			continue
		}
		svr.recvUpdate(msg.Key, msg.Val, msg.Id, msg.Counter, msg.Vec, msg.Sender)
	}
}