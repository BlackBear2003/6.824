package raft

import "time"

func (rf *Raft) raiseBroadcast(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if term != rf.currentTerm {
		return
	}
	if rf.state != LeaderState {
		PrettyDebug(dWarn, "S%d is no longer a Leader, stop rasing broadcast", rf.me)
		return
	}
	PrettyDebug(dLeader, "S%d raise broadcast to every peer at Term:%d", rf.me, term)

	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		preLogIndex := rf.nextIndex[peer] - 1
		lastLogIndex, _ := rf.getLastLogInfo()
		if preLogIndex < rf.lastIncludedIndex { // peer落后太多了，已经在snapshot里了，只能直接发动installSnapshot了
			args := &InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				Data:              rf.persister.snapshot,
			}
			go rf.installSnapshotHandler(args, peer)
		} else {
			args := &AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: preLogIndex,
				PrevLogTerm:  rf.getLog(preLogIndex).Term,
				LeaderCommit: rf.commitIndex,
				Entries:      rf.getLogs(preLogIndex+1, lastLogIndex+1),
			}
			go rf.appendEntriesHandler(peer, term, args)
		}
	}

	rf.lastBroadcast = time.Now()
	rf.resetBroadcastTimeout()
	PrettyDebug(dLeader, "S%d broadcast to others, update broadcast time and timeout", rf.me)
}
