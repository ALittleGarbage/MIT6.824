package raft

// startInstallSnapshot 开始向每个节点发送日志快照
func (rf *Raft) startInstallSnapshot() {
}

// executeInstallSnapshot 处理某个节点的快照响应
func (rf *Raft) executeInstallSnapshot(serverId int, args *InstallSnapshotArgs) {

}

// handleInstallSnapshot 客户端处理快照请求
func (rf *Raft) handleInstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {

}
