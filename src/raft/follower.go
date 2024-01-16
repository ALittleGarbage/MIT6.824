package raft

// handlerVote 处理选举请求，是否投票，响应Candidate
func (rf *Raft) handlerVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	// 请求的term小于等于自己的term，丢弃直接返回
	if args.Term <= rf.currentTerm {
		return
	}
	// 请求term大于自己的term，换成请求的term，重置自己的投票
	rf.currentTerm = args.Term
	rf.status = Follower
	rf.votedFor = -1
	rf.restElectionTime()

	lastLog := rf.getLastLog()
	// 还要判断日志是否日志，对方的日志大于我 或者 如果不大于则判断最后一个日志是否一致
	judgeLog := args.LastLogTerm > lastLog.Term || (args.LastLogTerm == lastLog.Term && args.LastLogIndex >= lastLog.Index)
	if judgeLog { // 同意投票
		rf.votedFor = args.CandidateId
		rf.status = Follower
		reply.VoteGranted = true
		DPrintf("%d 投给了 %d\n", rf.me, args.CandidateId)
	} else { //拒绝投票
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
	rf.persist()
}

// handlerAppendEntries 处理心跳请求，响应Leader
func (rf *Raft) handlerAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.NextIndex = len(rf.logs)

	// 请求的term大于自己的term，改为请求的term
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.persist()
	}
	// 如果请求term小于自己的term，丢弃直接返回
	if args.Term < rf.currentTerm {
		return
	}

	// 重置心跳时间
	rf.restHeartBeat()
	rf.status = Follower

	// 自己最后一个log依旧小于请求的前一个日志（至少应该相等），说明数据不一致
	if rf.getLastLog().Index < args.PrevLogIndex {
		return
	}
	// 自己所对应的日志的term不等于请求的任期，出现数据不一致
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		xTerm := rf.logs[args.PrevLogIndex].Term
		// 从后面开始找，找到第一个与xTerm不等于的索引，所以后面的全是与xTerm一样term的，全部替换
		for xIndex := args.PrevLogIndex; xIndex > 0; xIndex-- {
			if rf.logs[xIndex-1].Term != xTerm {
				reply.NextIndex = xIndex
				break
			}
		}
		return
	}
	// 同步日志
	if len(args.Entries) > 0 && args.Entries[0].Index > 0 {
		entry := args.Entries[0]
		rf.logs = rf.logs[:entry.Index]
		rf.logs = append(rf.logs, args.Entries...)
		rf.persist()
	}
	reply.NextIndex = len(rf.logs)
	reply.Success = true
	// 提交日志
	if args.LeaderCommit > rf.commitIndex {
		// 获取到已提交的日志索引和自己目前最大索引中的最小的那个
		rf.commitIndex = MinInt(args.LeaderCommit, rf.getLastLog().Index)
	}
}
