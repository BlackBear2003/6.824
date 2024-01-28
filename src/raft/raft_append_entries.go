package raft

import "sort"

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if len(args.Entries) == 0 {
		PrettyDebug(dTerm, "S%d received heartbeat from S%d of Term:%d", rf.me, args.LeaderId, args.Term)
	} else {
		PrettyDebug(dTerm, "S%d received AppendEntries(len=%d) from S%d of Term:%d", rf.me, len(args.Entries), args.LeaderId, args.Term)
	}

	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term < rf.currentTerm {
		// refuse because has higher term
		PrettyDebug(dLog2, "S%d 's Term:%d higher than leader(>%d), refused RPC", rf.me, rf.currentTerm, args.Term)
		return
	}
	rf.resetHeartbeatenTimeout()
	if args.Term == rf.currentTerm && rf.state == CandidateState {
		rf.updateTermPassively(rf.currentTerm)
	}
	if args.Term > rf.currentTerm {
		rf.updateTermPassively(args.Term)
		rf.leaderId = args.LeaderId
	}
	// 2B
	reply.Term = rf.currentTerm
	lastLogIndex, _ := rf.getLastLogInfo()
	// 先匹配上相交点
	if args.PrevLogIndex > lastLogIndex {
		PrettyDebug(dLog2, "S%d 's log(index:%d) smaller than leader(index:%d), refused RPC", rf.me, lastLogIndex, args.PrevLogIndex)
		return
	}

	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm && args.PrevLogTerm != 0 {
		PrettyDebug(dLog2, "S%d check prev term not matched", rf.me)
		return
	}

	reply.Success = true
	// prev log check pass, recognize each's logs before previndex are consist
	// 从PrevLogIndex开始，找出自己的日志中与Leader对应位置处不一致的日志，将此日志后续的所有日志删除，并将新的日志添加到后面
	PrettyDebug(dLog2, "S%d check prev success, replicate entries(len=%d) start from prev index:%d", rf.me, len(args.Entries), args.PrevLogIndex)
	if args.PrevLogIndex < rf.commitIndex {
		rf.commitIndex = args.PrevLogIndex
	}
	for i, entry := range args.Entries {

		idx := args.PrevLogIndex + i + 1
		lastLogIndex, _ = rf.getLastLogInfo()
		PrettyDebug(dLog2, "S%d replicate from entries -> index:%d", rf.me, idx)
		if idx == lastLogIndex+1 {
			rf.logs = append(rf.logs, entry)
		} else {
			// duplicated part
			if rf.logs[idx].Term != entry.Term {
				// not consist for term (MATCHING PREPERTIES)
				rf.logs = rf.logs[:idx]
				rf.logs = append(rf.logs, args.Entries[i:]...)
			}
		}
	}
	lastLogIndex, _ = rf.getLastLogInfo()
	PrettyDebug(dLog2, "S%d replicate log end to index:%d", rf.me, lastLogIndex)
	// success replicated logs
	if args.LeaderCommit > rf.commitIndex {
		PrettyDebug(dLog2, "S%d commitIndex:%d is smaller than leader:%d", rf.me, rf.commitIndex, args.LeaderCommit)
		// leader commits more than follower used to, should update commitIndex to min of two below
		if args.LeaderCommit > args.PrevLogIndex+len(args.Entries) {
			rf.commitIndex = args.PrevLogIndex + len(args.Entries)
			PrettyDebug(dLog2, "S%d set commitIndex to last new log's index:%d", rf.me, rf.commitIndex)
		} else {
			rf.commitIndex = args.LeaderCommit
			PrettyDebug(dLog2, "S%d set commitIndex to leader commit:%d", rf.me, rf.commitIndex)
		}
	}
}

func (rf *Raft) appendEntriesHandler(peer int, term int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != LeaderState {
		PrettyDebug(dWarn, "S%d is not Leader, this RPC -> S%d is useless", rf.me, peer)
		return
	}
	if len(args.Entries) == 0 {
		PrettyDebug(dLeader, "S%d sended heartbeat to S%d", rf.me, peer)
	} else {
		PrettyDebug(dLeader, "S%d sended AppendEntries(len=%d) to S%d", rf.me, len(args.Entries), peer)
	}
	if reply.Term < rf.currentTerm {
		PrettyDebug(dLeader, "S%d receive lower Term:%d( < %d) from S%d", rf.me, reply.Term, rf.currentTerm, peer)
		return
	}

	// if term != rf.currentTerm {
	// 	PrettyDebug(dLog, "S%d requestTerm: %d, currentTerm: %d.", rf.me, args.Term, rf.currentTerm)
	// 	return
	// }

	if reply.Term > rf.currentTerm {
		PrettyDebug(dLeader, "S%d receive higher Term:%d( > %d) from S%d", rf.me, reply.Term, rf.currentTerm, peer)
		rf.updateTermPassively(reply.Term)
		return
	}
	if reply.Success {
		// Eventually nextIndex will reach a point where the leader and follower logs match.
		// the follower’s log is consistent with the leader’s
		PrettyDebug(dLeader, "S%d -> S%d AppendEntries with success", rf.me, peer)

		rf.nextIndex[peer] += len(args.Entries)
		rf.matchIndex[peer] = rf.nextIndex[peer] - 1
		PrettyDebug(dLog, "S%d set S%d nextIndex=%d matchIndex=%d", rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer])
		// update leader's commitIndex
		// by calculate the index of majority could keep up with
		sortedMatchIndex := make([]int, 0)
		sortedMatchIndex = append(sortedMatchIndex, len(rf.logs))
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			sortedMatchIndex = append(sortedMatchIndex, rf.matchIndex[i])
		}
		sort.Ints(sortedMatchIndex)
		newCommitIndex := sortedMatchIndex[len(rf.peers)/2]
		PrettyDebug(dCommit, "S%d newCommitIndex:%d commitIndex:%d at Term:%d", rf.me, newCommitIndex, rf.commitIndex, rf.currentTerm)
		if newCommitIndex >= rf.commitIndex && rf.logs[newCommitIndex].Term == rf.currentTerm {
			rf.commitIndex = newCommitIndex
			PrettyDebug(dCommit, "S%d update commitIndex to %d", rf.me, rf.commitIndex)
		}
	} else {
		// After a rejection, the leader decrements nextIndex and retries the AppendEntries RPC.
		if rf.nextIndex[peer] > 1 {
			rf.nextIndex[peer]--
			PrettyDebug(dLog, "S%d (recv false) set S%d nextIndex=%d matchIndex=%d", rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer])
		}
	}
}
