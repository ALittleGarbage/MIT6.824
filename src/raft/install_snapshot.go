package raft

// startInstallSnapshot 开始向每个节点发送日志快照
func (rf *Raft) startInstallSnapshot() {
}

// executeInstallSnapshot 处理某个节点的快照响应
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

	rf.Snapshot(args.LastIncludeIndex, args.Data)
	reply.Term = rf.currentTerm
}
