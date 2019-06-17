package main

import (
	"fmt"
	"bytes"
	"encoding/gob"	
)

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
		msg = Message{OpType: ERR, Tv:TagVal{Tag:Tag{Id:"0",Ts:0}, Key:"", Val:""}}
	}
	return msg
}
