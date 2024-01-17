package raft

// startAppendEntries 向每个Follower发送心跳和日志
func (rf *Raft) startAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLog := rf.getLastLog()
	for i := range rf.peers {
		if i == rf.me {
			rf.restElectionTime()
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
		// 发送心跳以及日志
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
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		return
	}
	if req.Term == rf.currentTerm {
		if reply.Success {
			match := req.PrevLogIndex + len(req.Entries)
			next := match + 1
			rf.nextIndex[serverId] = MaxInt(rf.nextIndex[serverId], next)
			rf.matchIndex[serverId] = MaxInt(rf.matchIndex[serverId], match)
		} else {
			if reply.ConflictTerm == -1 {
				rf.nextIndex[serverId] = reply.NextIndex
			} else {
				index := reply.ConflictIndex
				for i := rf.getLastLog().Index; i > 0; i-- {
					term := rf.logs[i].Term
					if term == reply.ConflictTerm {
						index = i
					} else if term < reply.ConflictTerm {
						break
					}
				}
				rf.nextIndex[serverId] = index
			}
		}

		rf.leaderCommitLog()
	}
}

// handlerAppendEntries 处理心跳请求，响应Leader
func (rf *Raft) handlerAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm

	// 请求的term大于自己的term，改为请求的term
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		return
	}
	// 如果请求term小于自己的term，丢弃直接返回
	if args.Term < rf.currentTerm {
		return
	}

	// 重置心跳时间
	rf.restHeartBeat()
	if rf.state == Candidate {
		rf.state = Follower
	}

	lastLog := rf.getLastLog()
	// 自己最后一个log依旧小于请求的前一个日志（至少应该相等），说明数据不一致
	if lastLog.Index < args.PrevLogIndex {
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		reply.NextIndex = len(rf.logs)
		return
	}
	// 自己所对应的日志的term不等于请求的任期，出现数据不一致
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		xTerm := rf.logs[args.PrevLogIndex].Term
		for xIndex := args.PrevLogIndex; xIndex > 0; xIndex-- {
			if rf.logs[xIndex-1].Term != xTerm {
				reply.ConflictIndex = xIndex
				break
			}
		}
		reply.ConflictTerm = xTerm
		reply.NextIndex = len(rf.logs)
		return
	}
	// 同步日志
	if len(args.Entries) > 0 && args.Entries[0].Index > 0 {
		entry := args.Entries[0]
		rf.logs = rf.logs[:entry.Index]
		rf.logs = append(rf.logs, args.Entries...)
		rf.persist()
	}

	// 提交日志
	if args.LeaderCommit > rf.commitIndex {
		// 获取到已提交的日志索引和自己目前最大索引中的最小的那个
		rf.commitIndex = MinInt(args.LeaderCommit, rf.getLastLog().Index)
	}
	reply.Success = true
}
