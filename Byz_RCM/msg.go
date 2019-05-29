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
	Vec			[]int
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
		fmt.Println("Error occurred when decoding messesage in file msg.go", err)
		return Message{Kind: ERROR, Key: "", Val: "", Id: -1, Counter: -1, Vec: make([]int,1)}
	}
	return msg
}
