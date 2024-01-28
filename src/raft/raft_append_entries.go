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
	// 1. Reply false if term < currentTerm
	if args.Term < rf.currentTerm {
		PrettyDebug(dLog2, "S%d refused AE: leader Term:%d < current Term%d", rf.me, args.Term, rf.currentTerm)
		return
	}
	// 选举的时候发现已经有Leader，此时可能是竞争选举，认为自己失败，退回Follower
	if args.Term == rf.currentTerm && rf.state == CandidateState {
		PrettyDebug(dCandidate, "S%d receive leader Term:%d as a candidate, set back to follower", rf.me, args.Term, rf.currentTerm)
		rf.updateTermPassively(rf.currentTerm)
	}

	if args.Term > rf.currentTerm {
		rf.updateTermPassively(args.Term)
		rf.leaderId = args.LeaderId
	}

	// 此时可以确认对方是可以确认的Leader，刷新
	rf.resetHeartbeatenTimeout()

	// 2B
	reply.Term = rf.currentTerm
	lastLogIndex, _ := rf.getLastLogInfo()

	// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	// 先匹配上相交点
	if args.PrevLogIndex > lastLogIndex {
		PrettyDebug(dLog2, "S%d 's log(index:%d) shorter than prev index%d, refused new entries", rf.me, lastLogIndex, args.PrevLogIndex)
		return
	}
	if rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm && args.PrevLogTerm != 0 {
		PrettyDebug(dLog2, "S%d check prev term not matched", rf.me)
		return
	}

	reply.Success = true
	// prev log check pass, recognize each's logs before previndex are consist
	// 从PrevLogIndex开始，找出自己的日志中与Leader对应位置处不一致的日志，将此日志后续的所有日志删除，并将新的日志添加到后面
	if len(args.Entries) > 0 {
		PrettyDebug(dLog2, "S%d check prev success, replicate entries(len=%d) start from prev index:%d", rf.me, len(args.Entries), args.PrevLogIndex)
	}
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + i + 1
		lastLogIndex, _ = rf.getLastLogInfo()
		if idx == lastLogIndex+1 {
			// 4. Append any new entries not already in the log
			rf.logs = append(rf.logs, entry)
		}
		if rf.logs[idx].Term != entry.Term {
			PrettyDebug(dLog2, "S%d find diff entry from S%d at index:%d diffTerm(me:%d, leader:%d)",
				rf.me, args.LeaderId, idx, rf.logs[idx].Term, entry.Term)
			// 3. If an existing entry conflicts with a new one (same index but different terms),
			//    delete the existing entry and all that follow it
			rf.logs = rf.logs[:idx]
			// 4. Append any new entries not already in the log
			rf.logs = append(rf.logs, args.Entries[i:]...)
			lastLogIndex, _ = rf.getLastLogInfo()
			PrettyDebug(dLog2, "S%d append entries end to index:%d", rf.me, lastLogIndex)
			break
		}
	}
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		PrettyDebug(dLog2, "S%d commitIndex:%d < leader:%d, last new entry:%d", rf.me, rf.commitIndex, args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		PrettyDebug(dLog2, "S%d set commitIndex to %d", rf.me, rf.commitIndex)
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
		PrettyDebug(dLeader, "S%d receive outdated reply of lower Term:%d(me:%d) from S%d", rf.me, reply.Term, rf.currentTerm, peer)
		return
	}

	if reply.Term > rf.currentTerm {
		PrettyDebug(dLeader, "S%d receive higher Term:%d( > %d) from S%d", rf.me, reply.Term, rf.currentTerm, peer)
		rf.updateTermPassively(reply.Term)
		// rf.resetHeartbeatenTimeout()
		return
	}
	// reply.Term == currentTerm
	if reply.Success {
		// Eventually nextIndex will reach a point where the leader and follower logs match.
		// the follower’s log is consistent with the leader’s
		PrettyDebug(dLeader, "S%d -> S%d AppendEntries with success", rf.me, peer)
		peerNextIndex := args.PrevLogIndex + len(args.Entries) + 1
		peerMatchIndex := args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[peer] = max(rf.nextIndex[peer], peerNextIndex)
		rf.matchIndex[peer] = max(rf.matchIndex[peer], peerMatchIndex)
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
		if newCommitIndex >= rf.commitIndex && rf.logs[newCommitIndex].Term == rf.currentTerm {
			PrettyDebug(dCommit, "S%d newCommitIndex:%d > commitIndex:%d at Term:%d", rf.me, newCommitIndex, rf.commitIndex, rf.currentTerm)
			rf.commitIndex = newCommitIndex
			PrettyDebug(dCommit, "S%d update commitIndex to %d", rf.me, rf.commitIndex)
		}
		lastLogIndex, _ := rf.getLastLogInfo()
		for N := lastLogIndex; N > rf.commitIndex && rf.logs[N].Term == rf.currentTerm; N-- {
			count := 1
			for peer, matchIndex := range rf.matchIndex {
				if peer == rf.me {
					continue
				}
				if matchIndex >= N {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				PrettyDebug(dCommit, "S%d Updated commitIndex at T%d for majority consensus. commitIndex: %d.", rf.me, rf.currentTerm, rf.commitIndex)
				break
			}
		}
	} else {
		// After a rejection, the leader decrements nextIndex and retries the AppendEntries RPC.
		if rf.nextIndex[peer] > 1 {
			rf.nextIndex[peer]--
			PrettyDebug(dLog, "S%d (recv false) set S%d nextIndex=%d matchIndex=%d", rf.me, peer, rf.nextIndex[peer], rf.matchIndex[peer])
		}

		// lastLogIndex, _ := rf.getLastLogInfo()
		// preLogIndex := rf.nextIndex[peer] - 1
		// if lastLogIndex > preLogIndex {
		// 	PrettyDebug(dLog, "S%d <- S%d Inconsistent logs, retrying.", rf.me, peer)
		// 	newArgs := &AppendEntriesArgs{
		// 		Term:         rf.currentTerm,
		// 		LeaderId:     rf.me,
		// 		PrevLogTerm:  rf.logs[preLogIndex].Term,
		// 		PrevLogIndex: preLogIndex,
		// 		LeaderCommit: rf.commitIndex,
		// 		Entries:      append([]Entry{}, rf.logs[preLogIndex+1:]...),
		// 	}
		// 	go rf.appendEntriesHandler(peer, term, newArgs)
		// }
	}
}
