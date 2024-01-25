package raft

import (
	"log"
	"sync"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Counter struct {
	counter int
	mu      sync.Mutex
	cond    *sync.Cond
}

func (c *Counter) Increment() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.counter++
}

func (c *Counter) IncrementAndGet() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.counter++
	return c.counter
}

func (c *Counter) Decrement() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.counter--
}

func (c *Counter) Clear() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.counter = 0
}

func (c *Counter) GetValue() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.counter
}
