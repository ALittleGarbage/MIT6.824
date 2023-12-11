package raft

import (
	"log"
)

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func MaxInt(left int, right int) int {
	if left > right {
		return left
	}
	return right
}
func MinInt(left int, right int) int {
	if left < right {
		return left
	}
	return right
}
