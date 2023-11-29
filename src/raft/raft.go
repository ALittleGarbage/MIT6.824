package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

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
	ReqTypeVote = iota
	ReqTypeHeartBeat
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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	lastHeartBeat int64 // 最后一次心跳时间
	nodeType      int   // 节点类型
	term          int   // 任期
	votedFor      int   // 给谁投票了
}

// GetState 返回 任期 和 isLeader
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term, isLeader := rf.term, rf.nodeType == Leader
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// RequestVoteArgs RPC请求
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	ReqType int
	Term    int
	RfId    int
}

// RequestVoteReply RPC响应
type RequestVoteReply struct {
	// Your data here (2A).
	Status int
	Term   int
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	if args.ReqType == ReqTypeVote {
		rf.handlerVote(args, reply)
	}
	if args.ReqType == ReqTypeHeartBeat {
		rf.handlerHeartBeat(args, reply)
	}
}

// handlerVote 处理选举请求，是否投票，响应Candidate
func (rf *Raft) handlerVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.term { // 小于自己的任期，不理睬，直接返回
		reply.Status = 0
		reply.Term = rf.term
		return
	}
	if args.Term > rf.term { // 大于自己的任期，重置投票次数
		rf.term = args.Term
		rf.votedFor = -1
		rf.nodeType = Follower
	}
	// 大于自己的任期，投票跟随他，等于自己的任期，判断是否有投票次数
	reply.Term = rf.term
	if rf.votedFor == -1 { // 同意投票
		rf.votedFor = args.RfId
		rf.nodeType = Follower
		reply.Status = 1
		log.Printf("%d投给了%d\n", rf.me, args.RfId)
	} else { //拒绝投票
		reply.Status = 0
	}
}

// handlerHeartBeat 处理心跳请求，响应Leader
func (rf *Raft) handlerHeartBeat(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	// 自己的term大于请求的任期，旧任期直接丢弃
	if rf.term > args.Term {
		reply.Status = 0
		reply.Term = rf.term
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	rf.mu.Lock()
	// 真正的leader心跳请求，响应他
	rf.lastHeartBeat = time.Now().UnixNano() // 更新心跳
	rf.nodeType = Follower                   // 重置为follower
	if rf.term < args.Term {                 // 如果小于请求term
		rf.term = args.Term
		reply.Term = rf.term
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// Kill 自杀
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		if rf.nodeType != Leader && rf.isHeartbeatTimeout() {
			rf.startVote()
		}
		ms := 50 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		mu:            sync.Mutex{},
		peers:         peers,
		persister:     persister,
		me:            me,
		dead:          0,
		lastHeartBeat: 0,
		nodeType:      Follower,
		term:          0,
		votedFor:      -1,
	}

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}

// isHeartbeatTimeout 判断心跳是否超时，超时就可以参加选举了
func (rf *Raft) isHeartbeatTimeout() bool {
	return time.Now().UnixNano()-rf.lastHeartBeat > int64(time.Millisecond*50)
}

// startVote 参加选举，开始拉票
func (rf *Raft) startVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// 成为candidate 任期+1 先投票给自己
	rf.nodeType = Candidate
	rf.term++
	term := rf.term
	rf.votedFor = rf.me
	vote := 1
	flag := false
	req := RequestVoteArgs{Term: term, RfId: rf.me}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(serverId int) {
			reply := RequestVoteReply{}
			ok := rf.sendRequestVote(serverId, &req, &reply)
			if !ok || reply.Status == 0 {
				return
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			//丢弃无效票或者vote已经超过多数了，退出
			if rf.term > reply.Term || flag {
				return
			}
			vote++
			if vote > len(rf.peers)/2 {
				log.Printf("%d超过多数 vote:%d，成为leader\n", rf.me, vote)
				flag = true          // 之后的协程不需要再次判断vote了
				rf.nodeType = Leader // 将自身设置为leader
				go func() {          // 成为leader立刻开启心跳
					for rf.killed() == false && rf.nodeType == Leader {
						rf.sendHeartBeat()
						time.Sleep(time.Millisecond * 10)
					}
				}()
			}
		}(i)
	}
}

// sendHeartBeat 向每个Follower发送心跳
func (rf *Raft) sendHeartBeat() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	req := RequestVoteArgs{ReqType: ReqTypeHeartBeat, Term: rf.term, RfId: rf.me}
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, &req, &RequestVoteReply{})
	}
}
