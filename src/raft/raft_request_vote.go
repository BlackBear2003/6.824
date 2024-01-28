package raft

func (rf *Raft) requestVoteHandler(cnt *Counter, args *RequestVoteArgs, peer int, need int, me int) {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(peer, args, reply)
	if ok {
		rf.mu.Lock()
		if reply.VoteGranted {
			// It should have happened only once?
			if cnt.IncrementAndGet() == need {
				// become leader
				PrettyDebug(dVote, "S%d earned enough votes: %d at election of Term:%d", me, need, args.Term)

				if rf.state == CandidateState && rf.currentTerm == args.Term {
					rf.state = LeaderState
					rf.currentTerm++
					PrettyDebug(dLeader, "S%d become leader in Term:%d!", me, rf.currentTerm)
					// refresh timeout
					// begin := time.Now()
					// rf.lastElection = begin
					// 2B: initializeLogs when being leader
					lastLogIndex, _ := rf.getLastLogInfo()
					for peer := range rf.peers {
						// ??? need + 1 or not ???
						rf.nextIndex[peer] = lastLogIndex + 1
						rf.matchIndex[peer] = 0
					}
					PrettyDebug(dLog, "S%d set all peers' nextIndex=%d", me, lastLogIndex+1)
					// start heartbeat
					go rf.raiseBroadcast(rf.currentTerm)
				} else {
					PrettyDebug(dWarn, "S%d is no longer a candidate or in the new Term:%d, cannot raise to a leader", me, rf.currentTerm)
				}
			}
		}
		if reply.Term > args.Term {
			PrettyDebug(dVote, "S%d receive higher term(%d > %d) from S%d, set back to Follower", me, reply.Term, args.Term, peer)
			if rf.state == CandidateState && rf.currentTerm == args.Term {
				PrettyDebug(dCandidate, "S%d set back to Follower", me)
				rf.updateTermPassively(reply.Term)
				cnt.Clear()
			} else {
				PrettyDebug(dWarn, "S%d is not a candidate or in the new Term:%d, this won't affect", me, rf.currentTerm)
			}

		}
		rf.mu.Unlock()
	}
}

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() {
		reply.Term = 0
		reply.VoteGranted = false
		return
	}

	PrettyDebug(dVote, "S%d receive request vote from S%d of Term:%d", rf.me, args.CandidateId, args.Term)

	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		// just end
		PrettyDebug(dVote, "S%d's Term is higher(%d > %d), just reject this vote", rf.me, rf.currentTerm, args.Term)
		return
	}
	if args.Term > rf.currentTerm {
		rf.updateTermPassively(args.Term)
	}
	// never voted OR has voted to RPC's original peer
	// (I guess it is for the '每次RPC都是幂等的' so when duplicate send or receive, it act as same)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		lastLogIndex, lastLogTerm := rf.getLastLogInfo()
		PrettyDebug(dVote, "S%d never voted OR has voted to S%d", rf.me, args.CandidateId)
		if lastLogTerm < args.Term ||
			(lastLogTerm == args.Term && lastLogIndex <= args.LastLogIndex) {
			PrettyDebug(dVote, "S%d <- S%d(at least up to date), vote for S%d!", rf.me, args.CandidateId, args.CandidateId)
			reply.VoteGranted = true
			rf.votedFor = args.CandidateId
			// heartbeaten only if voted for candidate
			rf.state = FollowerState
			rf.resetHeartbeatenTimeout()
		} else {
			reply.VoteGranted = false
			PrettyDebug(dVote, "S%d not vote for S%d for more up-to-date", rf.me, args.CandidateId)
		}
	} else {
		reply.VoteGranted = false
		PrettyDebug(dVote, "S%d has voted for S%d, reject this RPC from S%d", rf.me, rf.votedFor, args.CandidateId)
	}
}

type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 2A
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
