package raft

import (
	"fmt"
	"log"
	"os"
	"runtime"
	"sync"
)

// Debugging
const Debug = false

var (
	logFile, _ = os.Create("debug.log")
	mu         sync.Mutex
)

func writeLog(format string, a ...interface{}) {
	mu.Lock()
	defer mu.Unlock()
	logFile.WriteString(fmt.Sprintf(format, a...))
}

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
		writeLog(format, a...)
	}
}

func GPrintf(format string, a ...interface{}) {
	if false {
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

func GetFunName() string {
	pc, _, _, _ := runtime.Caller(1)
	return runtime.FuncForPC(pc).Name()
}
