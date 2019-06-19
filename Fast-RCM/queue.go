package main

import (
	"sync"
)

type QueueEntry struct {
	Key		string
	Val 	string
	Id 		int
	Vec		[]int
}

type Queue struct {
	lock	*sync.Mutex
	values	[]QueueEntry
}

func (q *Queue) Init() {
	q.lock = &sync.Mutex{}
	q.values = make([]QueueEntry, 0)
}

func (q *Queue) Enqueue(entry QueueEntry) {
    q.lock.Lock()
    q.values = append(q.values, entry)
    q.lock.Unlock()

}

func (q *Queue) Dequeue() *QueueEntry {
  q.lock.Lock()
  if len(q.values) > 0 {
    entry := q.values[0]
    q.values = q.values[1:]
    q.lock.Unlock()
    return &entry
  }
  q.lock.Unlock()
  return nil
}

func (q *Queue) Len() int {
	return len(q.values)
}