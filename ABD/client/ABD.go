package main

import "fmt"

// abd write
func write(key string, val string){
	tv := get(key)
	tv.update(val)
	set(tv)
}

// abd read
func read(key string) string{
	tv := get(key)
	set(tv)
	return tv.Val
}

// get phase
func get(key string) TagVal {
	dealer := createDealerSocket()
	defer dealer.Close()

	// init tagVal
	tv := TagVal{Tag: Tag{Id: "", Ts: -1}, Key: key, Val: ""}
	msg := Message{OpType: GET, Tv: tv}
	sendToServer(msg, dealer)
	
	// find tagVal with biggest tag in quorum
	for i := 0; i < len(servers)/2 + 1; i++ {
		tmp := recvData(dealer)
		if tv.Tag.smaller(tmp.Tag) {
			tv = tmp
		}
	}
	fmt.Println("recv tv",tv.Tag,tv.Key)
	return tv
}

// set phase
func set(tv TagVal){
	dealer := createDealerSocket()
	defer dealer.Close()

	fmt.Println("set tag", tv.Tag, "key",tv.Key)
	msg := Message{OpType: SET, Tv: tv}
	sendToServer(msg, dealer)
	// recv ack from quorum
	for i := 0; i < len(servers)/2 + 1; i++ {
		recvAck(dealer)
	}
}
