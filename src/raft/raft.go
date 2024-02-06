package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"math"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type RaftState int

const (
	FollowerState RaftState = iota
	CandidateState
	LeaderState
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state RaftState

	// follower
	lastHeartbeaten    time.Time
	heartBeatenTimeout time.Duration

	// candidate
	lastElection    time.Time
	electionTimeout time.Duration
	// leader
	lastBroadcast    time.Time
	broadcastTimeout time.Duration

	// 2A
	// persistent state
	currentTerm int
	votedFor    int     // -1 means nil
	logs        []Entry // index from 1

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on leaders
	nextIndex  []int // for each server, index of the next log entry to send to that server
	matchIndex []int // for each server, index of highest log entry known to be replicated on server

	// 2B
	applyChannel chan ApplyMsg
	leaderId     int
}

type Entry struct {
	Term    int
	Command interface{}
}

// Need Lock
// return last's index and term
// index = 0 means no logs
func (rf *Raft) getLastLogInfo() (int, int) {
	if len(rf.logs) < 1 {
		return 0, 0
	}
	lastLogIndex := len(rf.logs) - 1
	lastLogTerm := rf.logs[lastLogIndex].Term
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) getLogIndexesWithTerm(term int) (firstIdx, lastIdx int) {
	if term == 0 {
		return 0, 0
	}
	first, last := math.MaxInt, -1
	for i := 1; i < len(rf.logs); i++ {
		if rf.logs[i].Term == term {
			first = min(first, i)
			last = max(last, i)
		}
	}
	if last == -1 {
		return -1, -1
	}
	return first, last
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == LeaderState)
	return term, isleader
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// invoke when receiving higher term peer's RPC
// !!!!! Need Lock !!!!!
// * update term
// * into follower state
// * vote for no one
func (rf *Raft) updateTermPassively(newTerm int) {
	PrettyDebug(dTerm, "S%d's Term is lower, updating term to Term:%d, setting state to follower. (%d > %d)",
		rf.me, newTerm, newTerm, rf.currentTerm)
	rf.currentTerm = newTerm
	rf.state = FollowerState
	// only voted once in a term
	rf.votedFor = -1
	rf.persist()
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	if rf.killed() {
		return index, term, false
	}

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = rf.state == LeaderState
	if !isLeader {
		// isn't the leader, returns false
		return index, term, false
	}
	// otherwise start the agreement and return immediately.
	e := Entry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs, e)
	rf.persist()
	index, term = rf.getLastLogInfo()
	PrettyDebug(dClient, "S%d receive client command, append to logs index:%d Term:%d", rf.me, index, term)

	//go rf.raiseBroadcast(term)
	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	PrettyDebug(dError, "S%d was killed", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	// 2A
	rf.state = FollowerState
	rf.resetHeartbeatenTimeout()

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]Entry, 0)
	rf.logs = append(rf.logs, Entry{})

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// 2B
	rf.applyChannel = applyCh

	// 2C
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	lastLogIndex, _ := rf.getLastLogInfo()
	for peer := range rf.peers {
		rf.nextIndex[peer] = lastLogIndex + 1
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.applyLogHandler(applyCh)

	return rf
}
