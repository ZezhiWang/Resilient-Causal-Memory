package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/peterbourgon/diskv"
)

var d = diskv.New(diskv.Options{
	BasePath:     "data",
})

type TagVal struct {
	ts []int
	val string
}

func isEqual(tv1 TagVal, tv2 TagVal) bool {
	if tv1.val != tv2.val{
		return false
	}

	return vecIsEqual(tv1.ts,tv2.ts)
}

func vecIsEqual(vec1 []int, vec2[]int) bool{
	if len(vec1) != len(vec2){
		return false
	}

	for i := 0; i < len(vec1); i++ {
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
		return TagVal{}
	}
	return ety
}

func storeToDisk(key string, ety *TagVal){
	b := getGobFromEntry(ety)
	if err := d.Write(key,b); err != nil{
		fmt.Println("cannot write")
	}
}

func readFromDisk(key string) TagVal {
	b,_ := d.Read(key)
	return getEntryFromGob(b)
}