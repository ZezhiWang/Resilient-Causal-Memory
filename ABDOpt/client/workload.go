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
const READ_PORTION = 0.3

// write info into table before read, with out tracking time
func initWrite(num int){
	// write data in the form (string,blob) into table tmp
	for i:= 0; i < num; i++{
		key := strconv.Itoa(i)
		write(key, strings.Repeat(strconv.Itoa(i % 10), DATASIZE))
	}
}

// write info into table
func write_load(num int, val string) time.Duration {
	// write data in the form (int, string) into table tmp
	// mutex.Lock()
	key := strconv.Itoa(num)
	start := time.Now()
	write(key, val)
	end := time.Now()
	// mutex.Unlock()
	elapsed := end.Sub(start)
	return elapsed
}

// read info from table by key
func read_load(num int) time.Duration {
	// write data in the form table tmp with key = num
	// mutex.Lock()
	key := strconv.Itoa(num)
	start := time.Now()
	read(key)
	end := time.Now()
	// mutex.Unlock()
	elapsed := end.Sub(start)
	return elapsed
}

func workload(num int){
	var write_times []int
	var read_times []int

	num_read := 0
	num_write := 0

	var WTotal, RTotal int = 0, 0

	// insert value into table before start testing
	initWrite(10)


	start := time.Now()
	for i := 0; i < num; i++ {
		temp := rand.Float64()
		if temp < READ_PORTION {
			fmt.Println("reading...")
			millisec := int(read_load(i%10).Nanoseconds()/1000)
			RTotal += millisec
			read_times = append(read_times, millisec)
			num_read += 1
		} else {
			fmt.Println("writing...")
			millisec := int(write_load(i%10, strings.Repeat(strconv.Itoa(i % 10), DATASIZE)).Nanoseconds()/1000)
			WTotal += millisec
			write_times = append(write_times, millisec)
			num_write += 1
		}
	}
	end := time.Now()
	totalTime := end.Sub(start)

	sort.Ints(write_times)
	sort.Ints(read_times)

	fmt.Printf("Thorough put: %f op/sec\n", float64(num_read + num_write) / float64(totalTime.Seconds()))
	fmt.Printf("Avg write time: %f us\n", float64(WTotal)/float64(num_write))
	fmt.Printf("Avg read time: %f us\n", float64(RTotal)/float64(num_read))
	fmt.Printf("95-th percentile for write time: %d us\n", write_times[int(float64(num_write) * 0.95)])
	fmt.Printf("95-th percentile for read time: %d us\n", read_times[int(float64(num_read) * 0.95)])
}
