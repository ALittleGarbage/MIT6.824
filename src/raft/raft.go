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

	applyCh chan ApplyMsg

	lastHeartBeat time.Time // 最后一次心跳时间
	electionTime  time.Time

	state       int // 节点类型
	currentTerm int // 任期
	votedFor    int // 投票的候选者id
	logs        []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	lastIncludeIndex int
	lastIncludeTerm  int
}

// GetState 返回 任期 和 isLeader
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term, isLeader := rf.currentTerm, rf.state == Leader
	return term, isLeader
}

// persist 持久化节点状态
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(&rf.currentTerm) != nil ||
		e.Encode(&rf.votedFor) != nil ||
		e.Encode(&rf.logs) != nil ||
		e.Encode(&rf.lastIncludeIndex) != nil ||
		e.Encode(&rf.lastIncludeTerm) != nil {
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
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.logs) != nil ||
		d.Decode(&rf.lastIncludeIndex) != nil ||
		d.Decode(&rf.lastIncludeTerm) != nil {
		DPrintf("%d 读取日志失败\n", rf.me)
	}
}

// Snapshot 安装快照数据，同时抛弃快照中的数据（你都安装快照就不需要存储在logs中了）
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	if rf.killed() {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// index小于上一次快照下标，直接返回
	if rf.lastIncludeIndex >= index || index > rf.commitIndex {
		return
	}

	rf.persister.Save(nil, snapshot)

	for idx, l := range rf.logs {
		if l.Index == index {
			// 这样做，添加一个默认log，防止越界
			temp := make([]LogEntry, 1)
			rf.logs = append(temp, rf.logs[idx+1:]...)
			rf.lastIncludeIndex = index
			rf.lastIncludeTerm = l.Term
			break
		}
	}
	DPrintf("%d 安装完成快照\n", rf.me)
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
	DPrintf("leader:%d 收到命令cmd:%v index:%d\n", rf.me, command, index)
	return index, term, true
}

// Kill 自杀
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	DPrintf("%d killed\n", rf.me)
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
			rf.startRequestVote()
		}
		time.Sleep(AppendEntriesTime)
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	size := len(peers)
	rf := &Raft{
		mu:               sync.Mutex{},
		peers:            peers,
		persister:        persister,
		me:               me,
		dead:             0,
		applyCh:          applyCh,
		lastHeartBeat:    time.Now(),
		electionTime:     time.Now(),
		state:            Follower,
		currentTerm:      0,
		votedFor:         -1,
		logs:             make([]LogEntry, 1), // 初始化添加一个默认空命令
		commitIndex:      0,
		lastApplied:      0,
		nextIndex:        make([]int, size),
		matchIndex:       make([]int, size),
		lastIncludeIndex: 0,
		lastIncludeTerm:  0,
	}

	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()
	go rf.applyLog()

	return rf
}

// leaderCommitLog leader更新可以提交的日志
func (rf *Raft) leaderCommitLog() {
	for idx := rf.commitIndex; idx <= rf.getLastLog().Index; idx++ {
		if rf.logs[idx].Term != rf.currentTerm {
			continue
		}
		counter := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= idx {
				counter++
			}
			if counter > len(rf.peers)/2 {
				rf.commitIndex = idx
				rf.persist()
				break
			}
		}
	}
}

// applyLog 日志提交到应用层
func (rf *Raft) applyLog() {
	for !rf.killed() {
		rf.mu.Lock()
		for idx, l := range rf.logs {
			// 超出已提交索引直接退出
			if l.Index > rf.commitIndex {
				break
			}
			// 已经提交到应用层的直接跳过
			if l.Index <= rf.lastApplied {
				continue
			}
			entry := rf.logs[idx]
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
			rf.applyCh <- applyMsg
			rf.lastApplied = idx
			DPrintf("节点:%d 提交命令 index:%d cmd:%v\n", rf.me, entry.Index, entry.Command)
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 5)
	}
}

// applySnapshot 快照同步到应用层
func (rf *Raft) applySnapshot() {
	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.persister.ReadSnapshot(),
		SnapshotTerm:  rf.lastIncludeTerm,
		SnapshotIndex: rf.lastIncludeIndex,
	}
	// 已经将快照同步，更新提交进度
	rf.lastApplied = rf.lastIncludeIndex
	rf.applyCh <- applyMsg
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
