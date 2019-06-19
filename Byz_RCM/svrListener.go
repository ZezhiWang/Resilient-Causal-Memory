package main

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"log"
)

func (svr *Server) serverTask(svrAddr string) {
	// Set the ZMQ sockets
	frontend,_ := zmq.NewSocket(zmq.ROUTER)
	defer frontend.Close()

	if err := frontend.Bind("tcp://" + svrAddr); err != nil{
		fmt.Println("err binding", svrAddr)
	}

	//  Backend socket talks to workers over inproc
	backend, _ := zmq.NewSocket(zmq.DEALER)
	defer backend.Close()

	if err := backend.Bind("inproc://backend"); err != nil {
		fmt.Println("err binding backend")
	}

	for i := 0; i < NumWorker; i++ {
		go svr.serverWorker()
	}

	//  Connect backend to frontend via a proxy
	err := zmq.Proxy(frontend, backend, nil)
	log.Fatal("Proxy interrupted:", err)
}

func (svr *Server) serverWorker() {
	worker, _ := zmq.NewSocket(zmq.DEALER)
	defer fmt.Println("worker socket closed")
	defer worker.Close()

	if err := worker.Connect("inproc://backend"); err != nil{
		fmt.Println("err connecting router")
	}
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
			output = svr.recvRead(input.Key, input.Id, input.Counter, input.Ts)
		case WRITE:
			output = svr.recvWrite(input.Key, input.Val, input.Id, input.Counter, input.Ts)
		case CHECK:
			output = svr.recvCheck(input.Key, input.Val, input.Counter, input.Ts)
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
		svr.recvUpdate(msg.Key, msg.Val, msg.Id, msg.Counter, msg.Ts, msg.Sender)
	}
}