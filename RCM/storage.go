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

	if len(tv1.ts) != len(tv2.ts){
		return false
	}

	for i := 0; i < len(tv1.ts); i++ {
		if tv1.ts[i] != tv2.ts[i]{
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