package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/peterbourgon/diskv"
	"strconv"
)

var h = diskv.New(diskv.Options{
	BasePath:     "hist" + strconv.Itoa(nodeId),
	CacheSizeMax: 10*1024,
})

//var(
//	h = make(map[string][]TagVal)
//	histLock = sync.Mutex{}
//	)

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
		return []TagVal{}
	}
	return etys
}

func histToDisk(key string, etys *[]TagVal){
	b := getGobFromHist(etys)
	if err := h.Write(key,b); err != nil{
		fmt.Println("cannot write")
	}
}

func histFromDisk(key string) ([]TagVal,error) {
	if b,err := h.Read(key); err == nil {
		return getHistFromGob(b), err
	} else {
		return []TagVal{}, err
	}
}

func histAppend(key string, tv *TagVal) {
	//histLock.Lock()
	if hist, err := histFromDisk(key); err == nil {
		if len(hist) == NumClient {
			hist = hist[1:]
		}
		hist = append(hist, *tv)
		histToDisk(key,&hist)
	} else {
		histToDisk(key,&hist)
	}
	//histLock.Unlock()
}