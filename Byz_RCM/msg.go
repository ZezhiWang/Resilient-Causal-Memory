package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

type Message struct {
	Kind		int
	Id			int
	Key			string
	Val			string
	Ts      	[NUM_CLIENT]int // vector timestamp of server or client
	Vec			[NUM_CLIENT]int // vector timestamp associate with key and value
	Counter		int
	Sender		int
}

// encode msg
func getGobFromMsg(msg *Message) []byte {
	var res bytes.Buffer

	enc := gob.NewEncoder(&res)
	if err := enc.Encode(&msg); err != nil {
		fmt.Println(err)
	}
	return res.Bytes()
}

// decode msg
func getMsgFromGob(msgBytes []byte) Message {
	var buff bytes.Buffer
	var msg Message

	buff.Write(msgBytes)
	dec := gob.NewDecoder(&buff)
	if err := dec.Decode(&msg); err != nil {
		fmt.Println("Error occurred when decoding message in file msg.go", err)
		return Message{Kind: ERROR, Key: "", Val: "", Id: -1, Counter: -1, Vec: [NUM_CLIENT]int{0,0}, Ts: [NUM_CLIENT]int{0,0}}
	}
	return msg
}
