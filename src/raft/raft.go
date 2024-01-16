package raft

import (
	"6.5840/labgob"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	Follower = iota
	Candidate
	Leader
)

const (
	AppendEntriesTime = time.Millisecond * 100
	HeartbeatTimeout  = time.Second
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	applyCh       chan ApplyMsg
	lastHeartBeat time.Time // 最后一次心跳时间
	electionTime  time.Time
	state         int // 节点类型
	currentTerm   int // 任期
	votedFor      int // 投票的候选者id
	logs          []LogEntry
	commitIndex   int
	lastApplied   int
	nextIndex     []int
	matchIndex    []int
}

// GetState 返回 任期 和 isLeader
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term, isLeader := rf.currentTerm, rf.state == Leader
	return term, isLeader
}

// persist 持久化日志
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(&rf.currentTerm) != nil || e.Encode(&rf.votedFor) != nil || e.Encode(&rf.logs) != nil {
		DPrintf("%d 持久化日志失败\n", rf.me)
	}
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
}

// readPersist 读取日志
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, voteFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&voteFor) != nil || d.Decode(&logs) != nil {
		DPrintf("%d 读取日志失败\n", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = voteFor
		rf.logs = make([]LogEntry, len(logs))
		copy(rf.logs, logs)

	}
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// Start 由客户端调用用来添加新的命令到本地日志中，leader将其复制到集群中其他节点上
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}

	index := rf.getLastLog().Index + 1
	term := rf.currentTerm

	entry := LogEntry{Index: index, Term: term, Command: command}
	rf.logs = append(rf.logs, entry)

	return index, term, true
}

// Kill 自杀
func (rf *Raft) Kill() {
	DPrintf("%d killed\n", rf.me)
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// ticker 选举或者发送心跳日志
func (rf *Raft) ticker() {
	for rf.killed() == false {
		if rf.state == Leader {
			rf.startAppendEntries()
		}
		if rf.state != Leader && rf.isElectionTimeout() && rf.isHeartTimeout() {
			rf.startVote()
		}
		time.Sleep(AppendEntriesTime)
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	size := len(peers)
	rf := &Raft{
		mu:           sync.Mutex{},
		peers:        peers,
		persister:    persister,
		me:           me,
		dead:         0,
		applyCh:      applyCh,
		electionTime: time.Now(),
		state:        Follower,
		currentTerm:  0,
		votedFor:     -1,
		logs:         make([]LogEntry, 0),
		commitIndex:  0,
		lastApplied:  0,
		nextIndex:    make([]int, size),
		matchIndex:   make([]int, size),
	}
	// 初始化一个默认空命令
	rf.logs = append(rf.logs, LogEntry{0, 0, nil})
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.apply()

	return rf
}

// apply 执行提交操作
func (rf *Raft) apply() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex > rf.lastApplied && rf.getLastLog().Index > rf.lastApplied {
			rf.lastApplied++
			entry := rf.logs[rf.lastApplied]
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
			rf.applyCh <- applyMsg
			DPrintf("%d 已提交命令 %d\n", rf.me, entry.Command)
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 100)
	}
}

// getLastLog 获取最后一个日志
func (rf *Raft) getLastLog() LogEntry {
	return rf.logs[len(rf.logs)-1]
}

// isHeartTimeout 判断心跳是否超时
func (rf *Raft) isHeartTimeout() bool {
	return time.Now().After(rf.lastHeartBeat)
}

// restHeartBeat 重置心跳
func (rf *Raft) restHeartBeat() {
	rf.lastHeartBeat = time.Now().Add(HeartbeatTimeout)
}

// isElectionTimeout 判断选举是否超时（不能固定选举，要随机停顿，避免同时选举）
func (rf *Raft) isElectionTimeout() bool {
	return time.Now().After(rf.electionTime)
}

// restElectionTime 重置选举时间
func (rf *Raft) restElectionTime() {
	extra := time.Duration(50+rand.Int63()%300) * time.Millisecond
	rf.electionTime = time.Now().Add(extra)
}
