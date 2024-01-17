package raft

import (
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
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
