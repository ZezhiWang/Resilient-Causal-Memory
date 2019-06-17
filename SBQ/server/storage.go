package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

func store(tv TagVal) Message {
	var res = Message{	ERR, TagVal{Ts:-1,Key:tv.Key,Val:""}}
	if local,err := readData(tv.Key); err != nil || local.Ts <= tv.Ts{
		writeData(tv)
		res.OpType = ACK
	}
	return res
}

func get(tv TagVal) Message{
	var res Message
	res.OpType = GET
	if local,err := readData(tv.Key); err == nil{
		res.Tv = local
	} else {
		res.Tv = TagVal{Ts: -1, Key: tv.Key, Val: ""}
	}
	return res
}

func getTs(tv TagVal) Message{
	var res Message
	res.OpType = GETTS
	res.Tv = TagVal{Ts: -1, Key: tv.Key, Val: ""}
	if local,err := readData(tv.Key); err == nil{
		res.Tv.Ts = local.Ts
	}
	return res
}

func writeData(tv TagVal) {
	var res bytes.Buffer
	var localTs = -1
	if local,err := readData(tv.Key); err == nil {
		localTs = local.Ts
	}

	if localTs > tv.Ts {
		return
	}

	enc := gob.NewEncoder(&res)
	if err := enc.Encode(tv); err != nil {
		fmt.Println(err)
	}
	if err := d.Write(tv.Key,res.Bytes()); err != nil{
		panic(err)
	}
}

func readData(key string) (TagVal,error) {
	var buff bytes.Buffer
	var tv TagVal
	b, err := d.Read(key)
	if err != nil{
		return TagVal{Ts:0,Key:key,Val:""}, err
	}
	buff.Write(b)
	dec := gob.NewDecoder(&buff)
	if err := dec.Decode(&tv); err != nil {
		fmt.Println(err)
	}
	return tv,nil
}