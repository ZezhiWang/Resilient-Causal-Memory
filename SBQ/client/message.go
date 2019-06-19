package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

type TagVal struct {
	Ts 		int
	Key 	string
	Val		string
}

type Message struct {
	OpType 	int
	Tv 		TagVal
}

// encode msg
func getGobFromMsg(msg Message) bytes.Buffer {
	var res bytes.Buffer

	enc := gob.NewEncoder(&res)
	if err := enc.Encode(msg); err != nil {
		fmt.Println(msg)
	}
	return res
}

// decode msg
func getMsgFromGob(msgBytes []byte) Message {
	var buff bytes.Buffer
	var msg Message

	buff.Write(msgBytes)
	dec := gob.NewDecoder(&buff)
	if err := dec.Decode(&msg); err != nil {
		msg = Message{OpType: ERR, Tv:TagVal{Ts:-1, Key:"", Val:""}}
	}
	return msg
}
