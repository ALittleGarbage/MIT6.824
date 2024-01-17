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

func GetCmd(logs []LogEntry) []interface{} {
	cmds := make([]interface{}, len(logs))

	for i, l := range logs {
		cmds[i] = l.Command
	}

	return cmds
}
