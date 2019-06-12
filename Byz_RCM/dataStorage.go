package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/peterbourgon/diskv"
	"strconv"
)

var d = diskv.New(diskv.Options{
	BasePath:     "data" + strconv.Itoa(nodeId),
})

type TagVal struct {
	Ts  [NUM_CLIENT]int
	Val string
}

func isEqual(tv1 TagVal, tv2 TagVal) bool {
	if tv1.Val != tv2.Val {
		return false
	}

	return vecIsEqual(tv1.Ts,tv2.Ts)
}

func vecIsEqual(vec1 [NUM_CLIENT]int, vec2[NUM_CLIENT]int) bool{
	//if len(vec1) != len(vec2){
	//	return false
	//}

	for i := 0; i < NUM_CLIENT; i++ {
		if vec1[i] != vec2[i]{
			return false
		}
	}

	return true
}

func getGobFromEntry(ety *TagVal) []byte {
	var res bytes.Buffer

	enc := gob.NewEncoder(&res)
	if err := enc.Encode(&ety); err != nil {
		fmt.Println(err)
	}
	return res.Bytes()
}

func getEntryFromGob(etyBytes []byte) TagVal {
	var buff bytes.Buffer
	var ety TagVal

	buff.Write(etyBytes)
	dec := gob.NewDecoder(&buff)
	if err := dec.Decode(&ety); err != nil {
		fmt.Println("Error occurred when decoding Entry", err)
		return TagVal{Ts: [2]int{0,0}, Val: ""}
	}
	return ety
}

func storeToDisk(key string, ety *TagVal){
	b := getGobFromEntry(ety)
	if err := d.Write(key,b); err != nil{
		fmt.Println("cannot write")
	}
	histAppend(key, ety)
}

func readFromDisk(key string) TagVal {
	if b, err := d.Read(key); err == nil {
		return getEntryFromGob(b)
	}
	return TagVal{Ts: [2]int{0,0}, Val: ""}
}