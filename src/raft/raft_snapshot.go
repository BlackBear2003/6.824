package raft

import (
	"bytes"

	"6.824/labgob"
)

func (rf *Raft) persistAndSnapshot(snapshot []byte) {
	// Your code here (2C).
	PrettyDebug(dPersist, "S%d Saving persistent state to stable storage at Term:%d.", rf.me, rf.currentTerm)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	PrettyDebug(dSnap, "S%d Snapshotting through index %d.", rf.me, index)

	lastLogIndex, _ := rf.getLastLogInfo()
	if rf.lastIncludedIndex >= index {
		PrettyDebug(dSnap, "S%d Snapshot already applied to persistent storage. (%d >= %d)", rf.me, rf.lastIncludedIndex, index)
		return
	}

	PrettyDebug(dSnap, "S%d logs:%v   IncludeIndex:%d IncludeTerm:%d", rf.me, rf.logs, rf.lastIncludedIndex, rf.lastIncludedTerm)
	rf.lastIncludedTerm = rf.getLog(index).Term
	rf.logs = rf.copyLogs(index+1, lastLogIndex+1)
	rf.lastIncludedIndex = index
	PrettyDebug(dSnap, "S%d after cutting logs:%v   IncludeIndex:%d IncludeTerm:%d", rf.me, rf.logs, rf.lastIncludedIndex, rf.lastIncludedTerm)

	rf.persistAndSnapshot(snapshot)

	lastLogIndex, _ = rf.getLastLogInfo()
	PrettyDebug(dSnap, "S%d was snapshoted, lastIncludedIndex:%d lastIncludedTerm:%d lastLogIndex:%d",
		rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, lastLogIndex)
}

// InstallSnapshotArgs is the InstallSnapshot RPC arguments structure.
// field names must start with capital letters!
type InstallSnapshotArgs struct {
	Term              int    // leader's term
	LeaderId          int    // so follower can redirect clients
	LastIncludedIndex int    // the snapshot replaces all entries up through and including this index
	LastIncludedTerm  int    // term of lastIncludedIndex
	Data              []byte // raw bytes of the snapshot chunk, starting at offset
}

// InstallSnapshotReply is the InstallSnapshot RPC reply structure.
// field names must start with capital letters!
type InstallSnapshotReply struct {
	Term int // currentTerm, for leader to update itself
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	return true
}

/**
 * 当 Leader 尝试给一个非常落后的 Follower 节点发送 AppendEntries 日志的时候，
 * 可能会发现自己已经捞不出那块日志了（因为已经被截断拉去做快照了，现在的截断位置已经比这个落后节点的 matchIndex 来的高了），
 * 这个时候 Leader 节点就转为向这个节点发送一个 InstallSnapshot RPC，要求这个落后的 Follower 节点通过直接安装快照的方式来更新自己的状态。
 */

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	PrettyDebug(dSnap, "S%d <- S%d Received install snapshot request at Term:%d.", rf.me, args.LeaderId, rf.currentTerm)

	if args.Term < rf.currentTerm {
		PrettyDebug(dSnap, "S%d Term is higher(%d < %d), rejecting install snapshot request. ", rf.me, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.updateTermPassively(args.Term)
	}
	reply.Term = rf.currentTerm
	rf.state = FollowerState
	rf.resetHeartbeatenTimeout()

	if rf.commitIndex >= args.LastIncludedIndex {
		PrettyDebug(dSnap, "S%d receive outdated snapshot. (IncludeIndex:%d >= snapshotInclude:%d)", rf.me, rf.lastIncludedIndex, args.LastIncludedIndex)
		return
	}

	// 将快照后的logs切割，快照前的直接applied
	index := args.LastIncludedIndex
	tempLogs := make([]Entry, 0)
	tempLogs = append(tempLogs, Entry{0, nil})
	lastLogIndex, _ := rf.getLastLogInfo()

	if index <= lastLogIndex {
		// 快照并不能包含接收者的所有的日志条目
		// 把快照包含的条目都裁剪掉，留下快照不包含的
		tempLogs = append(tempLogs, rf.getLogs(index+1, lastLogIndex+1)...)
	}

	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.logs = tempLogs
	lastLogIndex, _ = rf.getLastLogInfo()
	PrettyDebug(dSnap, "S%d installed snapshot, lastIncludedIndex:%d lastIncludedTerm:%d lastLogIndex:%d",
		rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm, lastLogIndex)
	rf.lastApplied = rf.lastIncludedIndex
	rf.commitIndex = rf.lastIncludedIndex
	rf.persistAndSnapshot(args.Data)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	go func() {
		rf.applyChannel <- msg
	}()
}

func (rf *Raft) installSnapshotHandler(args *InstallSnapshotArgs, peer int) {
	reply := &InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(peer, args, reply)
	if ok {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		PrettyDebug(dSnap, "S%d <- S%d Received install snapshot reply at Term:%d.", rf.me, peer, rf.currentTerm)

		if rf.currentTerm != args.Term {
			PrettyDebug(dWarn, "S%d Term has changed after the install snapshot request, install snapshot reply discarded. "+
				"requestTerm:%d, currentTerm:%d.", rf.me, args.Term, rf.currentTerm)
			return
		}
		if rf.currentTerm < reply.Term {
			rf.updateTermPassively(reply.Term)
			return
		}

		rf.matchIndex[peer] = max(args.LastIncludedIndex, rf.matchIndex[peer])
		rf.nextIndex[peer] = max(args.LastIncludedIndex+1, rf.nextIndex[peer])
	}
}
