package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/peterbourgon/diskv"
)
var h = diskv.New(diskv.Options{
	BasePath:     "hist",
})

func getGobFromHist(etys *[]TagVal) []byte {
	var res bytes.Buffer

	enc := gob.NewEncoder(&res)
	if err := enc.Encode(&etys); err != nil {
		fmt.Println(err)
	}
	return res.Bytes()
}

func getHistFromGob(etyBytes []byte) []TagVal {
	var buff bytes.Buffer
	var etys []TagVal

	buff.Write(etyBytes)
	dec := gob.NewDecoder(&buff)
	if err := dec.Decode(&etys); err != nil {
		fmt.Println("Error occurred when decoding history", err)
		return make([]TagVal,0)
	}
	return etys
}

func histToDisk(key string, etys *[]TagVal){
	b := getGobFromHist(etys)
	if err := h.Write(key,b); err != nil{
		fmt.Println("cannot write")
	}
}

func histFromDisk(key string) []TagVal {
	b,_ := h.Read(key)
	return getHistFromGob(b)
}

func histAppend(key string, tv TagVal) {
	hist := histFromDisk(key)
	hist = append(hist,tv)
	histToDisk(key,&hist)
}