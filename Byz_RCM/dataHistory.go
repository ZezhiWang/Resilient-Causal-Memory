package main

//var h = diskv.New(diskv.Options{
//	BasePath:     "hist" + strconv.Itoa(nodeId),
//})

var h = make(map[string][]TagVal)

//func getGobFromHist(etys *[]TagVal) []byte {
//	var res bytes.Buffer
//
//	enc := gob.NewEncoder(&res)
//	if err := enc.Encode(&etys); err != nil {
//		fmt.Println(err)
//	}
//	return res.Bytes()
//}

//func getHistFromGob(etyBytes []byte) []TagVal {
//	var buff bytes.Buffer
//	var etys []TagVal
//
//	buff.Write(etyBytes)
//	dec := gob.NewDecoder(&buff)
//	if err := dec.Decode(&etys); err != nil {
//		fmt.Println("Error occurred when decoding history", err)
//		return []TagVal{}
//	}
//	return etys
//}

//func histToDisk(key string, etys *[]TagVal){
//	b := getGobFromHist(etys)
//	if err := h.Write(key,b); err != nil{
//		fmt.Println("cannot write")
//	}
//}

//func histFromDisk(key string) []TagVal {
//	if b,err := h.Read(key); err == nil {
//		return getHistFromGob(b)
//	}
//	return []TagVal{}
//}

func histAppend(key string, tv *TagVal) {
	if hist, isIn := h[key]; isIn {
		if len(hist) == NUM_CLIENT {
			h[key] = hist[1:]
		}
		h[key] = append(hist, *tv)
	} else {
		h[key] = []TagVal{*tv}
	}
}