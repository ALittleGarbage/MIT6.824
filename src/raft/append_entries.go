package raft

// startAppendEntries 向每个Follower发送心跳和日志
func (rf *Raft) startAppendEntries() {
	GPrintf("%d 开始上锁 %s\n", rf.me, GetFunName())
	rf.mu.Lock()
	GPrintf("%d 上锁成功 %s\n", rf.me, GetFunName())
	defer func() {
		GPrintf("%d 解锁成功 %s\n", rf.me, GetFunName())
	}()
	defer rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			rf.restElectionTime()
			continue
		}
		// 如果i节点的nextIndex已经在快照中了，那么直接发送快照，快速同步
		if rf.nextIndex[i] <= rf.lastIncludeIndex {
			go rf.executeInstallSnapshot(i)
			continue
		}

		nextIndex := rf.nextIndex[i]
		if nextIndex <= 0 {
			nextIndex = 1
		}
		// 获取上一个日志
		preLog := rf.getLogByIndex(nextIndex - 1)
		if preLog.Index == 0 && rf.lastIncludeIndex != 0 {
			preLog = LogEntry{
				Index:   rf.lastIncludeIndex,
				Term:    rf.lastIncludeTerm,
				Command: nil,
			}
		}
		req := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: preLog.Index,
			PrevLogTerm:  preLog.Term,
			Entries:      rf.logs[rf.getPosByIndex(nextIndex):],
			LeaderCommit: rf.commitIndex,
		}
		// 发送心跳以及日志
		go rf.executeAppendEntries(i, &req)
	}
}

// executeAppendEntries 处理某个节点的日志响应
func (rf *Raft) executeAppendEntries(serverId int, args *AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(serverId, args, &reply)
	if !ok { // 发送失败
		return
	}

	GPrintf("%d 开始上锁 %s\n", rf.me, GetFunName())
	rf.mu.Lock()
	GPrintf("%d 上锁成功 %s\n", rf.me, GetFunName())
	defer func() {
		GPrintf("%d 解锁成功 %s\n", rf.me, GetFunName())
	}()
	defer rf.mu.Unlock()

	DPrintf("%d 收到 %d 心跳响应\n reply:[%+v]\n", rf.me, serverId, reply)

	// 自己的term小于响应的任期，变成follower
	if rf.currentTerm < reply.Term {
		rf.state = Follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persist()
		return
	}
	// 等待响应期间并没上锁，有可能term已经更新了
	if args.Term == rf.currentTerm {
		if reply.Success {
			match := 0
			if len(args.Entries) > 0 {
				match = args.Entries[len(args.Entries)-1].Index
			}
			next := match + 1
			rf.nextIndex[serverId] = MaxInt(rf.nextIndex[serverId], next)
			rf.matchIndex[serverId] = MaxInt(rf.matchIndex[serverId], match)
		} else {
			if reply.ConflictTerm == -1 {
				rf.nextIndex[serverId] = reply.NextIndex
			} else {
				index := reply.ConflictIndex
				for i := rf.getLastLogIdx(); i > 0; i-- {
					term := rf.getLogByIndex(i).Term
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

// handleAppendEntries 处理心跳请求，响应Leader
func (rf *Raft) handleAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	GPrintf("%d 开始上锁 %s\n", rf.me, GetFunName())
	rf.mu.Lock()
	GPrintf("%d 上锁成功 %s\n", rf.me, GetFunName())
	defer func() {
		GPrintf("%d 解锁成功 %s\n", rf.me, GetFunName())
	}()
	defer rf.mu.Unlock()

	defer func() {
		DPrintf("%d 收到 %d 心跳\n "+
			" lastApplied:%d mineCommitIndex:%d mineLastIncludeIndex:%d\n "+
			" mineLog:[%+v]\n "+
			" arg:[%+v]\n"+
			" reply:[%+v]\n",
			rf.me, args.LeaderId,
			rf.lastApplied, rf.commitIndex, rf.lastIncludeIndex,
			rf.logs, args,
			reply)

	}()
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

	// 自己最后一个log依旧小于请求的前一个日志（至少应该相等），说明数据不一致
	if rf.getLastLogIdx() < args.PrevLogIndex {
		reply.ConflictIndex = -1
		reply.ConflictTerm = -1
		reply.NextIndex = rf.getLastLogIdx() + 1
		return
	}

	preLog := rf.getLogByIndex(args.PrevLogIndex)
	xTerm := preLog.Term
	// 如果preLog的index为0，则说明logs中只有一个默认命令，刚刚安装快照，logs被释放了，需要重新获取preLog
	if preLog.Index == 0 && rf.lastIncludeIndex != 0 {
		xTerm = rf.lastIncludeTerm
	}
	// 自己所对应的日志的term不等于请求的任期，出现数据不一致
	if xTerm != args.PrevLogTerm {
		for xIndex := args.PrevLogIndex; xIndex > 0; xIndex-- {
			if rf.getLogByIndex(xIndex-1).Term != xTerm {
				reply.ConflictIndex = xIndex
				break
			}
		}
		reply.ConflictTerm = xTerm
		reply.NextIndex = rf.getLastLogIdx() + 1
		return
	}
	// 同步日志
	if len(args.Entries) > 0 && args.Entries[0].Index > 0 {
		entry := args.Entries[0]
		rf.logs = rf.logs[:rf.getPosByIndex(entry.Index)]
		rf.logs = append(rf.logs, args.Entries...)
		rf.persist()
	}

	// 提交日志
	if rf.commitIndex < args.LeaderCommit {
		// 获取到已提交的日志索引和自己目前最大索引中的最小的那个
		rf.commitIndex = MinInt(args.LeaderCommit, rf.getLastLogIdx())
		// 开始交付日志
		rf.apply()
	}
	reply.Success = true
}
