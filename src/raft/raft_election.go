package raft

import (
	"sync"
)

func (rf *Raft) raiseElection() {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == LeaderState {
		return
	}

	rf.resetElectionTimeout()
	rf.currentTerm++
	rf.state = CandidateState
	rf.votedFor = rf.me

	PrettyDebug(dVote, "S%d raising an election at Term:%d", rf.me, rf.currentTerm)

	cnt := &Counter{
		counter: 0,
		mu:      sync.Mutex{},
	}
	cnt.Increment()
	need := len(rf.peers)/2 + 1
	lastLogIndex, lastLogTerm := rf.getLastLogInfo()
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	// call each peer
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		PrettyDebug(dVote, "S%d sending requestVote to S%d, Term:%d", rf.me, peer, rf.currentTerm)
		go rf.requestVoteHandler(cnt, args, peer, need, rf.me)
	}

	// the last time of this election
	// time.Sleep(electionTimeout)
	// time out and just stop this election
}
