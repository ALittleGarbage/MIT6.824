package raft

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int //候选者最后一条日志记录的索引
	LastLogTerm  int //候选者最后一条日志记录的索引的任期
}

type RequestVoteReply struct {
	VoteGranted bool
	Term        int
}

type AppendEntriesArgs struct {
	Term         int        // 任期
	LeaderId     int        // leader的id
	PrevLogIndex int        // 前继日志记录的索引
	PrevLogTerm  int        // 前继日志的任期
	Entries      []LogEntry // 存储日志记录
	LeaderCommit int        // leader 的commitIndex
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

// sendRequestVote 发送拉票请求
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// sendAppendEntries 发送心跳请求
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// RequestVote Vote handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.handlerVote(args, reply)
}

// AppendEntries HeartBeat handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.handlerAppendEntries(args, reply)
}
