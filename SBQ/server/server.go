package main 

import (
	"fmt"
	zmq "github.com/pebbe/zmq4"
	"log"
)

func server_task() {
	// Set the ZMQ sockets
	frontend,_ := zmq.NewSocket(zmq.ROUTER)
	defer frontend.Close()
	frontend.Bind("tcp://"+addr)

	//  Backend socket talks to workers over inproc
	backend, _ := zmq.NewSocket(zmq.DEALER)
	defer backend.Close()
	backend.Bind("inproc://backend")

	go serverWorker()

	//  Connect backend to frontend via a proxy
	err := zmq.Proxy(frontend, backend, nil)
	log.Fatal("Proxy interrupted:", err)
}

func serverWorker() {
	worker, _ := zmq.NewSocket(zmq.DEALER)
	defer worker.Close()
	worker.Connect("inproc://backend")
	msgReply := make([][]byte, 2)

	for i := 0; i < len(msgReply); i++ {
		msgReply[i] = make([]byte, 0) // the frist frame  specifies the identity of the sender, the second specifies the content
	}

	for {
		msg,err := worker.RecvMessageBytes(0)
		if err != nil {
			fmt.Println(err)
		}
		// decode message
		message := getMsgFromGob(msg[1])
		msgReply[0] = msg[0]

		// create response message
		tmpMsg, needResp := createRep(message)
		if needResp{
			// encode message
			tmpGob := getGobFromMsg(tmpMsg)
			msgReply[1] = tmpGob.Bytes()
			if _,err:= worker.SendMessage(msgReply); err != nil{
				fmt.Println("Err replying: ", err)
			}
		}
	}
}

// create response message
func createRep(msg Message) (Message,bool) {
	var flag = true
	switch msg.OpType{
	// if store phase
	case STORE:
		msg = store(msg.Tv)
	// if get
	case GET:
		msg = get(msg.Tv)
	// if get ts
	case GETTS:
		msg = getTs(msg.Tv)
	// if cannot read msg
	case ERR:
		flag = false
	}
	return msg,flag
}
