package raft

import "sync"

// startVote 参加选举，开始拉票
func (rf *Raft) startVote() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.status = Candidate // 成为candidate，先把票投给自己
	rf.currentTerm++      // 任期+1
	term := rf.currentTerm
	rf.votedFor = rf.me // 投给自己
	rf.restElectionTime()
	vote := 1 // 投票数+1
	becomeLeader := sync.Once{}
	lastLog := rf.getLastLog()
	req := RequestVoteArgs{
		Term:         term,
		CandidateId:  rf.me,
		LastLogIndex: lastLog.Index,
		LastLogTerm:  lastLog.Term,
	}
	for i := range rf.peers {
		if i == rf.me { // 跳过自己
			continue
		}
		go rf.executeVote(i, &vote, &becomeLeader, &req)
	}
	rf.persist()
}

// executeVote 处理某个节点投票的响应
func (rf *Raft) executeVote(serverId int, vote *int, becomeLeader *sync.Once, req *RequestVoteArgs) {
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(serverId, req, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if req.Term < reply.Term {
		rf.currentTerm = reply.Term
		rf.persist()
		return
	}
	if req.Term > reply.Term {
		return
	}
	// 更新完，term再判断是否投票成功
	if !reply.VoteGranted {
		return
	}
	*vote++
	if *vote > len(rf.peers)/2 && rf.currentTerm == req.Term && rf.status != Leader {
		becomeLeader.Do(func() { // 仅执行一次成为leader操作
			DPrintf("%d超过多数 term：%d vote:%d，成为leader\n", rf.me, rf.currentTerm, *vote)
			rf.status = Leader // 将自身设置为leader
			lastLog := rf.getLastLog()
			for i := range rf.peers { // 初始化follower的日志索引
				rf.nextIndex[i] = lastLog.Index + 1
				rf.matchIndex[i] = 0
			}
			go rf.startAppendEntries() // 成为leader立刻开启心跳
		})
	}
}

// startAppendEntries 向每个Follower发送心跳和日志
func (rf *Raft) startAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	lastLog := rf.getLastLog()
	for i := range rf.peers {
		if i == rf.me {
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
		// 立即发送心跳，镇压
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
		rf.currentTerm = reply.Term
		rf.status = Follower
		rf.persist()
		return
	}
	if req.Term == rf.currentTerm {
		if !reply.Success { // 更新失败的情况
			rf.nextIndex[serverId] = reply.NextIndex
			return
		} else { // 更新成功的情况
			match := req.PrevLogIndex + len(req.Entries)
			next := match + 1
			rf.nextIndex[serverId] = MaxInt(rf.nextIndex[serverId], next)
			rf.matchIndex[serverId] = MaxInt(rf.matchIndex[serverId], match)
		}
		rf.leaderCommitLog()
	}
}

// leaderCommitLog leader更新可以提交的日志
func (rf *Raft) leaderCommitLog() {
	for idx := rf.commitIndex + 1; idx <= rf.getLastLog().Index; idx++ {
		if rf.logs[idx].Term != rf.currentTerm {
			continue
		}
		counter := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= idx {
				counter++
			}
			if counter > len(rf.peers)/2 {
				rf.commitIndex = idx
				rf.persist()
				break
			}
		}
	}
}
