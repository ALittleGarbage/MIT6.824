package raft

import "sync"

// startRequestVote 参加选举，开始拉票
func (rf *Raft) startRequestVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.restElectionTime()
	rf.state = Candidate // 成为candidate，先把票投给自己
	rf.currentTerm++     // 任期+1
	term := rf.currentTerm
	rf.votedFor = rf.me // 投给自己
	rf.persist()
	vote := 1 // 投票数+1
	becomeLeader := sync.Once{}
	lastLog := rf.getLastLog()
	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	for i := range rf.peers {
		if i == rf.me { // 跳过自己
			continue
		}
		go rf.executeRequestVote(i, &vote, &becomeLeader, &args)
	}
}

// executeRequestVote 处理某个节点投票的响应
func (rf *Raft) executeRequestVote(serverId int, vote *int, becomeLeader *sync.Once, args *RequestVoteArgs) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverId, args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 请求的时term小于响应的term，变为follower
	if args.Term < reply.Term {
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		return
	}
	// 已经失效
	if args.Term > reply.Term {
		return
	}
	// 更新完，term再判断是否投票成功
	if !reply.VoteGranted {
		return
	}
	*vote++
	if *vote > len(rf.peers)/2 && rf.currentTerm == args.Term && rf.state == Candidate {
		becomeLeader.Do(func() { // 仅执行一次成为leader操作
			DPrintf("%d超过多数 term：%d vote:%d，成为leader\n", rf.me, rf.currentTerm, *vote)
			rf.state = Leader // 将自身设置为leader
			lastLog := rf.getLastLog()
			for i := range rf.peers { // 初始化follower的日志索引
				rf.nextIndex[i] = lastLog.Index + 1
				rf.matchIndex[i] = 0
			}
			go rf.startAppendEntries() // 成为leader立刻开启心跳
		})
	}
}

// handleRequestVote 处理选举请求，是否投票，响应Candidate
func (rf *Raft) handleRequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 请求term大于自己的term，换成请求的term，重置自己的投票
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	// 请求的term小于等于自己的term，丢弃直接返回
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	lastLog := rf.getLastLog()
	// 还要判断日志是否日志，对方的日志大于我 或者 如果不大于则判断最后一个日志是否一致
	judgeLog := args.LastLogTerm > lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)
	if judgeLog && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) { // 同意投票
		rf.votedFor = args.CandidateId
		rf.state = Follower
		reply.VoteGranted = true
		rf.persist()
		rf.restElectionTime()
		DPrintf("%d 投给了 %d\n", rf.me, args.CandidateId)
	} else { //拒绝投票
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
}
