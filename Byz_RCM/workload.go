package main 

import(
	"fmt"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"
)

// initialize mutex lock
// var mutex = &sync.Mutex{}
const ReadPortion = 0.3
const DataSize = 64

// write info into table before read, with out tracking time
func (clt *Client) initWrite(num int){
	// write data in the form (string,blob) into table tmp
	for i:= 0; i < num; i++{
		key := strconv.Itoa(i)
		clt.write(key, strings.Repeat(strconv.Itoa(i % 10), DataSize))
	}
}

// write info into table
func (clt *Client) writeLoad(num int, val string) time.Duration {
	// write data in the form (int, string) into table tmp
	key := strconv.Itoa(num)
	start := time.Now()
	clt.write(key, val)
	end := time.Now()
	elapsed := end.Sub(start)
	return elapsed
}

// read info from table by key
func (clt *Client) readLoad(num int) time.Duration {
	// write data in the form table tmp with key = num	
	key := strconv.Itoa(num)
	start := time.Now()
	clt.read(key)
	end := time.Now()
	elapsed := end.Sub(start)
	return elapsed
}

func (clt *Client) workload(num int){
	var writeTimes []int
	var readTimes []int

	numRead := 0
	numWrite := 0

	var WTotal, RTotal = 0, 0

	// insert value into table before start testing
	clt.initWrite(10)


	start := time.Now()
	for i := 0; i < num; i++ {
		temp := rand.Float64()
		if temp < ReadPortion {
			// fmt.Println("reading...")
			mSec := int(clt.readLoad(i%10).Nanoseconds()/1000)
			RTotal += mSec
			readTimes = append(readTimes, mSec)
			numRead += 1
		} else {
			// fmt.Println("writing...")
			mSec := int(clt.writeLoad(i%10, strings.Repeat(strconv.Itoa(i % 10), DataSize)).Nanoseconds()/1000)
			WTotal += mSec
			writeTimes = append(writeTimes, mSec)
			numWrite += 1
		}
	}
	end := time.Now()
	totalTime := end.Sub(start)

	sort.Ints(writeTimes)
	sort.Ints(readTimes)

	fmt.Printf("Thorough put: %f op/sec\n", float64(numRead+numWrite) / float64(totalTime.Seconds()))
	fmt.Printf("Avg write time: %f us\n", float64(WTotal)/float64(numWrite))
	fmt.Printf("Avg read time: %f us\n", float64(RTotal)/float64(numRead))
	fmt.Printf("95-th percentile for write time: %d us\n", writeTimes[int(float64(numWrite-1) * 0.95)])
	fmt.Printf("95-th percentile for read time: %d us\n", readTimes[int(float64(numRead-1) * 0.95)])
}
