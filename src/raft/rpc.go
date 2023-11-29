package raft

import (
	"log"
	"time"
)

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateId int
}

type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool
	Term        int
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// RequestVote Vote handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.handlerVote(args, reply)
}

// AppendEntries HeartBeat handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.handlerHeartBeat(args, reply)
}

// handlerVote 处理选举请求，是否投票，响应Candidate
func (rf *Raft) handlerVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.term { // 小于自己的任期，不理睬，直接返回
		reply.VoteGranted = false
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
	// 自己的term大于请求的任期，旧任期直接丢弃
	if rf.term > args.Term {
		reply.Success = false
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
