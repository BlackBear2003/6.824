package raft

import (
	"math/rand"
	"time"
)

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// 2A
		rf.mu.Lock()

		if rf.state == FollowerState {
			if time.Since(rf.lastHeartbeaten) > rf.heartBeatenTimeout {
				PrettyDebug(dTimer, "S%d's heartbeaten timeout, be candidate, Term:%d", rf.me, rf.currentTerm)
				rf.state = CandidateState
			}
		}

		if rf.state == CandidateState {
			if time.Since(rf.lastElection) > rf.electionTimeout {
				PrettyDebug(dTimer, "S%d's election timeout, raising an election, Term:%d", rf.me, rf.currentTerm)
				go rf.raiseElection()
			}
		}
		if rf.state == LeaderState {
			if time.Since(rf.lastBroadcast) > rf.broadcastTimeout {
				PrettyDebug(dTimer, "S%d's broadcast timeout, broadcasting for Term:%d", rf.me, rf.currentTerm)
				go rf.raiseBroadcast(rf.currentTerm)
			}
		}

		rf.mu.Unlock()

		// sleep for a tick
		tick := 30 * time.Millisecond
		time.Sleep(tick)
	}
}

// 一次选举的超时时间
// 只有在收到来自可以确认的Leader的心跳的时候才能刷新这个时长
func (rf *Raft) resetHeartbeatenTimeout() {
	rf.lastHeartbeaten = time.Now()
	rf.heartBeatenTimeout = time.Duration(rand.Intn(100)+200) * time.Millisecond
}

// 一次选举的超时时间
func (rf *Raft) resetElectionTimeout() {
	rf.lastElection = time.Now()
	rf.electionTimeout = time.Duration(rand.Intn(200)+400) * time.Millisecond
}

// 主动发起心跳的时间间隔
func (rf *Raft) resetBroadcastTimeout() time.Duration {
	rf.broadcastTimeout = time.Duration(rand.Intn(20)+100) * time.Millisecond
	return rf.broadcastTimeout
}
