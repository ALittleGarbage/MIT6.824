package raft

import (
	"log"
	"time"
)

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int //候选者最后一条日志记录的索引
	LastLogItem  int //候选者最后一条日志记录的索引的任期
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
	// Your code here (2A, 2B).
	rf.handlerVote(args, reply)
}

// AppendEntries HeartBeat handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.handlerHeartBeat(args, reply)
	if reply.Success {
		rf.handlerLog(args, reply)
	}
}

// handlerVote 处理选举请求，是否投票，响应Candidate
func (rf *Raft) handlerVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 小于自己的任期，不理睬，直接返回
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	//处理日志
	if args.LastLogIndex < rf.commitIndex {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	// 大于自己的任期，重置投票次数
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.nodeType = Follower
	}
	// 大于自己的任期，投票跟随他，等于自己的任期，判断是否有投票次数
	reply.Term = rf.currentTerm
	if rf.votedFor == -1 { // 同意投票
		rf.votedFor = args.CandidateId
		rf.nodeType = Follower
		reply.VoteGranted = true
		log.Printf("%d投给了%d\n", rf.me, args.CandidateId)
	} else { //拒绝投票
		reply.VoteGranted = false
	}
}

// handlerHeartBeat 处理心跳请求，响应Leader
func (rf *Raft) handlerHeartBeat(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	// 自己的term大于请求的任期，旧任期直接丢弃，并且日志索引大于请求的索引
	lastIndex := len(rf.logs) - 1
	if args.PrevLogIndex < lastIndex && rf.currentTerm > args.Term {
		reply.Success = false
		reply.Term = rf.currentTerm
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()

	// 真正的leader心跳请求，响应他
	rf.mu.Lock()
	rf.lastHeartBeat = time.Now().UnixNano() // 更新心跳
	rf.nodeType = Follower                   // 重置为follower
	if rf.currentTerm < args.Term {          // 如果小于请求term
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
	}
	reply.Success = true
	rf.mu.Unlock()
}

func (rf *Raft) handlerLog(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 判断日志是否正确
	if args.PrevLogIndex > 0 && len(rf.logs) > 0 &&
		(len(rf.logs) < args.PrevLogIndex ||
			rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm ||
			rf.commitIndex > args.PrevLogIndex) {
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.NextIndex = rf.commitIndex + 1
		return
	}
	// 判断是否是空日志
	if len(args.Entries) != 0 {
		// 添加日志
		rf.logs = rf.logs[:args.PrevLogIndex]
		rf.logs = append(rf.logs, args.Entries...)
	}

	// 提交日志
	for rf.commitIndex < args.LeaderCommit {
		entry := rf.logs[rf.commitIndex]
		applyMsg := ApplyMsg{
			CommandValid: true,
			CommandIndex: entry.Index,
			Command:      entry.Command,
		}
		rf.commitIndex++
		rf.lastApplied = rf.commitIndex
		rf.applyCh <- applyMsg
		log.Printf("follow %d 已提交 %d\n", rf.me, entry.Index)
	}
	// 更新nextIndex
	for i := 0; i < len(rf.nextIndex); i++ {
		rf.nextIndex[i] = rf.commitIndex
	}

	reply.Term = rf.currentTerm
	reply.Success = true
	reply.NextIndex = len(rf.logs)
}
