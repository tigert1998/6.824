package raft

import (
	"log"
	"sync/atomic"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func minInt(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func maxInt(x, y int) int {
	if x > y {
		return x
	}
	return y
}

func atomicMaxInt32(x *int32, y int32) {
	for {
		prevValue := atomic.LoadInt32(x)
		if prevValue >= y {
			break
		}
		if atomic.CompareAndSwapInt32(x, prevValue, y) {
			break
		}
	}
}
