package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
)

func set(tv TagVal) Message{
	if local,err := readData(tv.Key); err == nil || local.Tag.smaller(tv.Tag){
		writeData(tv)
	}
	return Message{OpType: SET, Tv: TagVal{Tag: Tag{Id: "", Ts: 0}, Key: "", Val: ""}}
}

func get(tv TagVal) Message{
	var res Message
	res.OpType = GET
	if local,err := readData(tv.Key); err == nil{
		res.Tv = local
	} else {
		res.Tv = TagVal{Tag: Tag{Id: "", Ts: 0}, Key: tv.Key, Val: ""}
	}
	return res
}

func writeData(tv TagVal) {
	var res bytes.Buffer
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
		return TagVal{Tag:Tag{Id:"",Ts:0},Key:key,Val:""}, err
	}
	buff.Write(b)
	dec := gob.NewDecoder(&buff)
	if err := dec.Decode(&tv); err != nil {
		fmt.Println(err)
	}
	return tv,nil
}