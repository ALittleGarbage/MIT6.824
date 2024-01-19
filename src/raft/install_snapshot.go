package raft

// startInstallSnapshot 开始向每个节点同步日志快照
func (rf *Raft) startInstallSnapshot() {
}

// executeInstallSnapshot 处理某个节点的快照同步
func (rf *Raft) executeInstallSnapshot(serverId int) {
	args := InstallSnapshotArgs{
		Term:             rf.currentTerm,
		LeaderId:         rf.me,
		LastIncludeIndex: rf.lastIncludeIndex,
		LastIncludeTerm:  rf.lastIncludeTerm,
		Data:             rf.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(serverId, &args, &reply)
	if !ok {
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
	if args.Term == rf.currentTerm {
		// 更新索引
		rf.nextIndex[serverId] = args.LastIncludeIndex + 1
		rf.matchIndex[serverId] = args.LastIncludeIndex
	}
	// 更新leader提交
	rf.leaderCommitLog()
}

// handleInstallSnapshot 客户端处理快照请求
func (rf *Raft) handleInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

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

	// index小于上一次快照下标，直接返回
	if rf.lastIncludeIndex >= args.LastIncludeIndex || args.LastIncludeIndex > rf.commitIndex {
		return
	}

	rf.persister.Save(nil, args.Data)

	// 如果节点的最后一个索引都小于快照的最后一个索引，直接清空
	if rf.getLastLog().Index < args.LastIncludeIndex {
		// 初始化一个默认的空命令
		rf.logs = make([]LogEntry, 1)
	} else {
		for idx, l := range rf.logs {
			if l.Index == args.LastIncludeIndex {
				// 初始化一个默认的空命令
				temp := make([]LogEntry, 1)
				rf.logs = append(temp, rf.logs[idx+1:]...)
				break
			}
		}
	}

	rf.lastIncludeIndex = args.LastIncludeIndex
	rf.lastIncludeTerm = args.LastIncludeTerm

	// 将快照交付到应用层
	rf.applySnapshot()

	reply.Term = rf.currentTerm
}
