package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
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
		fmt.Println(err)
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
		fmt.Println(err)
	}
	return msg
}
