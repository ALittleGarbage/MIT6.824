package raft

import (
	"log"
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
	lastHeartBeat int64 // 最后一次心跳时间
	nodeType      int   // 节点类型
	currentTerm   int   // 任期
	votedFor      int   // 投票的候选者id
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

	term, isLeader := rf.currentTerm, rf.nodeType == Leader
	return term, isLeader
}

func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// Start 由客户端调用用来添加新的命令到本地日志中，leader将其复制到集群中其他节点上
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	if rf.killed() {
		return -1, -1, false
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader = rf.nodeType == Leader
	if !isLeader {
		return -1, -1, false
	}

	term = rf.currentTerm
	index = len(rf.logs) + 1
	log.Printf("leader %d 收到命令 index：%d\n", rf.me, index)
	rf.logs = append(rf.logs, LogEntry{Index: index, Term: term, Command: command})
	rf.nextIndex[rf.me] = index + 1
	return index, term, isLeader
}

// Kill 自杀
func (rf *Raft) Kill() {
	log.Printf("%d killed\n", rf.me)
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.nodeType == Leader {
			rf.startAppendEntries()
			time.Sleep(AppendEntriesTime)
		}
		if rf.nodeType != Leader && rf.isHeartbeatTimeout() {
			ms := 50 + (rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)
			rf.startVote()
		}
		rf.mu.Unlock()
	}
}

func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	size := len(peers)
	rf := &Raft{
		mu:            sync.Mutex{},
		peers:         peers,
		persister:     persister,
		me:            me,
		dead:          0,
		applyCh:       applyCh,
		lastHeartBeat: 0,
		nodeType:      Follower,
		currentTerm:   0,
		votedFor:      -1,
		logs:          make([]LogEntry, 0),
		commitIndex:   0,
		lastApplied:   0,
		nextIndex:     make([]int, size),
		matchIndex:    make([]int, size),
	}
	// 初始化一个默认空命令
	rf.logs = append(rf.logs, LogEntry{0, 0, nil})
	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}

// isHeartbeatTimeout 判断心跳是否超时，超时就可以参加选举了
func (rf *Raft) isHeartbeatTimeout() bool {
	return time.Now().UnixNano()-rf.lastHeartBeat > int64(HeartbeatTimeout)
}

// startVote 参加选举，开始拉票
func (rf *Raft) startVote() {
	rf.nodeType = Candidate // 成为candidate，先把票投给自己
	rf.currentTerm++        // 任期+1
	term := rf.currentTerm
	rf.votedFor = rf.me // 投给自己
	vote := 1           // 投票数+1
	flag := false       // 是否已经取得vote结果
	lastLog := rf.getLastLog()
	req := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogItem:  lastLog.Term,
	}
	for i := range rf.peers {
		if i == rf.me { // 跳过自己
			continue
		}
		go rf.executeVote(i, &vote, &flag, &req)
	}
}

func (rf *Raft) executeVote(serverId int, vote *int, flag *bool, req *RequestVoteArgs) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverId, req, &reply)
	if !ok || !reply.VoteGranted {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	//丢弃无效票或者vote已经超过多数了，退出
	if rf.currentTerm > reply.Term || *flag {
		return
	}
	*vote++
	if *vote > len(rf.peers)/2 {
		log.Printf("%d超过多数 vote:%d，成为leader\n", rf.me, vote)
		*flag = true               // 之后的协程不需要再次判断vote了
		rf.nodeType = Leader       // 将自身设置为leader
		go rf.startAppendEntries() // 成为leader立刻开启心跳
	}
}

// startAppendEntries 向每个Follower发送心跳和日志
func (rf *Raft) startAppendEntries() {
	committed := 1
	flag := false
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.executeAppendEntries(i, &committed, &flag)
	}
}

func (rf *Raft) executeAppendEntries(serverId int, committed *int, flag *bool) {
	req := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: 0, // 前一个的索引，默认为nextIndex为0
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	req.Entries = rf.logs[rf.nextIndex[serverId]:]
	if rf.nextIndex[serverId] > 0 {
		entry := rf.logs[rf.nextIndex[serverId]-1]
		req.PrevLogIndex = entry.Index
		req.PrevLogTerm = entry.Term
	}

	reply := AppendEntriesReply{}
	rf.sendAppendEntries(serverId, &req, &reply)
	if !reply.Success && rf.currentTerm > reply.Term { // 心跳失败
		return
	}

	if !reply.Success { // 日志同步失败，说明日志存在缺少，更新nextIndex
		rf.nextIndex[serverId] = reply.NextIndex
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	*committed++
	if *committed > len(rf.peers)/2 && !*flag { // 超过半数，增加commitIndex
		*flag = true
		for rf.commitIndex < len(rf.logs) {
			entry := rf.logs[rf.commitIndex]
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: entry.Index,
			}
			rf.commitIndex++
			rf.lastApplied = rf.commitIndex
			rf.applyCh <- applyMsg
			log.Printf("leader %d 已提交 %d\n", rf.me, entry.Index)
		}
	}
	// 更新follower的nextIndex
	rf.nextIndex[serverId] = reply.NextIndex
}

func (rf *Raft) getLastLog() LogEntry {
	return rf.logs[len(rf.logs)-1]
}
