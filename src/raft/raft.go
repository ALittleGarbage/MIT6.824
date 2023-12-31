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
	AppendEntriesTime = time.Millisecond * 50
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
	status        int // 节点类型
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

	term, isLeader := rf.currentTerm, rf.status == Leader
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

	if rf.status != Leader {
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
		if rf.status == Leader {
			rf.startAppendEntries()
		}
		if rf.status != Leader && rf.isElectionTimeout() && rf.isHeartTimeout() {
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
		status:       Follower,
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

// startVote 参加选举，开始拉票
func (rf *Raft) startVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.status = Candidate // 成为candidate，先把票投给自己
	rf.currentTerm++      // 任期+1
	term := rf.currentTerm
	rf.votedFor = rf.me // 投给自己
	rf.restElectionTime()
	vote := 1 // 投票数+1
	becomeLeader := sync.Once{}
	lastLog := rf.getLastLog()
	req := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	for i := range rf.peers {
		if i == rf.me { // 跳过自己
			continue
		}
		go rf.executeVote(i, &vote, &becomeLeader, &req)
	}
	rf.persist()
}

// executeVote 处理某个节点投票的响应
func (rf *Raft) executeVote(serverId int, vote *int, becomeLeader *sync.Once, req *RequestVoteArgs) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverId, req, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if req.Term < reply.Term {
		rf.currentTerm = reply.Term
		rf.persist()
		return
	}
	if req.Term > reply.Term {
		return
	}
	// 更新完，term再判断是否投票成功
	if !reply.VoteGranted {
		return
	}
	*vote++
	if *vote > len(rf.peers)/2 && rf.currentTerm == req.Term && rf.status != Leader {
		becomeLeader.Do(func() { // 仅执行一次成为leader操作
			DPrintf("%d超过多数 term：%d vote:%d，成为leader\n", rf.me, rf.currentTerm, *vote)
			rf.status = Leader // 将自身设置为leader
			lastLog := rf.getLastLog()
			for i := range rf.peers { // 初始化follower的日志索引
				rf.nextIndex[i] = lastLog.Index + 1
				rf.matchIndex[i] = 0
			}
			go rf.startAppendEntries() // 成为leader立刻开启心跳
		})
	}
}

// startAppendEntries 向每个Follower发送心跳和日志
func (rf *Raft) startAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLog := rf.getLastLog()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		nextIndex := rf.nextIndex[i]
		if nextIndex <= 0 {
			nextIndex = 1
		}
		if lastLog.Index+1 < nextIndex {
			nextIndex = lastLog.Index
		}
		preLog := rf.logs[nextIndex-1] // 获取上一个日志
		req := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: preLog.Index,
			PrevLogTerm:  preLog.Term,
			Entries:      rf.logs[nextIndex:],
			LeaderCommit: rf.commitIndex,
		}
		// 立即发送心跳，镇压
		go rf.executeAppendEntries(i, &req)
	}
}

// executeAppendEntries 处理某个节点的日志响应
func (rf *Raft) executeAppendEntries(serverId int, req *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(serverId, req, &reply)
	if !ok { // 发送失败
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 自己的term小于响应的任期，变成follower
	if rf.currentTerm < reply.Term {
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.persist()
		return
	}
	if req.Term == rf.currentTerm {
		if !reply.Success { // 更新失败的情况
			rf.nextIndex[serverId] = reply.NextIndex
			return
		} else { // 更新成功的情况
			match := req.PrevLogIndex + len(req.Entries)
			next := match + 1
			rf.nextIndex[serverId] = MaxInt(rf.nextIndex[serverId], next)
			rf.matchIndex[serverId] = MaxInt(rf.matchIndex[serverId], match)
		}
		rf.leaderCommitLog()
	}
}

// leaderCommitLog leader更新可以提交的日志
func (rf *Raft) leaderCommitLog() {
	for idx := rf.commitIndex + 1; idx <= rf.getLastLog().Index; idx++ {
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
