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
	"math/rand"
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
	heartbeatenTimeout time.Duration
	// candidate
	lastElection    time.Time
	electionTimeout time.Duration
	// leader
	lastHeartbeat    time.Time
	heartbeatTimeout time.Duration

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
}

type Entry struct {
	Term    int
	Command interface{}
}

// 一次选举的超时时间
func (rf *Raft) randElectionTimeout() time.Duration {
	rf.electionTimeout = time.Duration(rand.Intn(300)+300) * time.Millisecond
	return rf.electionTimeout
}

// 主动发起心跳的时间间隔
func (rf *Raft) randHeartbeatTimeout() time.Duration {
	rf.heartbeatTimeout = time.Duration(rand.Intn(100)+100) * time.Millisecond
	return rf.heartbeatTimeout
}

// 收到心跳的最大间隔，超过视为失联
func (rf *Raft) heartbeaten() {
	rf.lastHeartbeaten = time.Now()
	rf.heartbeatenTimeout = time.Duration(rand.Intn(200)+200) * time.Millisecond
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

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	// 2A
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

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

// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	// 2A
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.heartbeaten()
	PrettyDebug(dVote, "S%d receive request vote from S%d", rf.me, args.CandidateId)

	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		// just end
		PrettyDebug(dVote, "S%d's Term is higher, just reject this vote", rf.me)
		return
	}
	if args.Term > rf.currentTerm {
		rf.updateTermPassively(args.Term)
	}
	// never voted OR has voted to RPC's original peer
	// (I guess it is for the '每次RPC都是幂等的' so when duplicate send or receive, it act as same)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		logLen := len(rf.logs)
		PrettyDebug(dVote, "S%d never voted OR has voted to S%d", rf.me, args.CandidateId)
		if rf.logs[logLen-1].Term < args.Term ||
			(rf.logs[logLen-1].Term == args.Term && logLen <= args.LastLogIndex) {
			PrettyDebug(dVote, "S%d is slower than S%d, grant vote for S%d!", rf.me, args.CandidateId, args.CandidateId)
			reply.VoteGranted = true
		} else {
			PrettyDebug(dVote, "S%d is not as up-to-date as receiver S%d, not voting", args.CandidateId, rf.me)
		}
	} else {
		PrettyDebug(dVote, "S%d has voted for S%d, reject this RPC from S%d", rf.me, rf.votedFor, args.CandidateId)
	}
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
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.heartbeaten()
	PrettyDebug(dTerm, "S%d received heartbeat from S%d of Term:%d", rf.me, args.LeaderId, args.Term)
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.state = FollowerState
	rf.currentTerm = args.Term
	// log part in 2B
}

func (rf *Raft) raiseHearbeat(term int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	PrettyDebug(dLeader, "S%d raise heartbeat to every peer at Term:%d", rf.me, term)

	if term != rf.currentTerm {
		return
	}
	if rf.state != LeaderState {
		PrettyDebug(dWarn, "S%d is no longer a Leader, stop rasing heartbeat", rf.me)
		return
	}

	args := &AppendEntriesArgs{
		Term:     term,
		LeaderId: rf.me,
	}
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		go rf.hearbeatToPeer(peer, term, args)
	}

	rf.lastHeartbeat = time.Now()
	rf.randHeartbeatTimeout()
	PrettyDebug(dLeader, "S%d heartbeat to others, update heartbeat time and timeout", rf.me)
}

func (rf *Raft) hearbeatToPeer(peer int, term int, args *AppendEntriesArgs) {
	reply := &AppendEntriesReply{}
	ok := rf.sendAppendEntries(peer, args, reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	PrettyDebug(dLeader, "S%d sended heartbeat to S%d", rf.me, peer)
	if reply.Term > rf.currentTerm {
		PrettyDebug(dLeader, "S%d receive higher Term:%d( > %d) from S%d", rf.me, reply.Term, rf.currentTerm, peer)
		rf.updateTermPassively(reply.Term)
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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

	// Your code here (2B).

	return index, term, isLeader
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// 2A
		rf.mu.Lock()

		if rf.state == FollowerState {
			if time.Since(rf.lastHeartbeaten) > rf.heartbeatenTimeout {
				PrettyDebug(dTimer, "S%d has lost heartbeaten, planning to be Candidate", rf.me)
				rf.state = CandidateState
			}
		}
		if rf.state == CandidateState {
			if time.Since(rf.lastElection) > rf.electionTimeout {
				PrettyDebug(dTimer, "S%d's last election timeout, raising an election", rf.me)
				go rf.raiseElection()
			}
		}
		if rf.state == LeaderState {
			if time.Since(rf.lastHeartbeat) > rf.heartbeatTimeout {
				PrettyDebug(dTimer, "S%d's last heartbeat timeout, heartbeating for term:%d", rf.me, rf.currentTerm)
				go rf.raiseHearbeat(rf.currentTerm)
			}
		}

		rf.mu.Unlock()

		// sleep for a tick
		tick := time.Duration(rand.Intn(10)+10) * time.Millisecond
		time.Sleep(tick)
	}
}

func (rf *Raft) raiseElection() {

	rf.mu.Lock()

	if rf.state == LeaderState {
		return
	}

	begin := time.Now()
	rf.lastElection = begin
	electionTimeout := rf.randElectionTimeout()
	rf.currentTerm++
	rf.state = CandidateState
	rf.votedFor = rf.me

	PrettyDebug(dCandidate, "S%d raising an election at Term:%d", rf.me, rf.currentTerm)

	cnt := &Counter{
		counter: 0,
		mu:      sync.Mutex{},
		cond:    sync.NewCond(&sync.Mutex{}),
	}
	cnt.Increment()
	need := len(rf.peers)/2 + 1
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.logs),
		LastLogTerm:  rf.logs[len(rf.logs)-1].Term,
	}

	// call each peer
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		PrettyDebug(dCandidate, "S%d sending requestVote to S%d, terms:%d", rf.me, peer, rf.currentTerm)
		go rf.requestVoteToPeer(cnt, args, peer, need, rf.me)
	}

	rf.mu.Unlock()

	// the last time of this election will be limit
	// for time.Since(begin) < electionTimeout && cnt.GetValue() < need {
	// 	time.Sleep(10 * time.Millisecond)
	// }
	time.Sleep(electionTimeout)

	// time out and just stop this election

}

func (rf *Raft) requestVoteToPeer(cnt *Counter, args *RequestVoteArgs, peer int, need int, me int) {
	reply := &RequestVoteReply{}
	ok := rf.sendRequestVote(peer, args, reply)
	if ok {
		if reply.VoteGranted {
			// It should have happened only once?
			if cnt.IncrementAndGet() == need {
				// become leader
				PrettyDebug(dLeader, "S%d earned enough votes: %d, become leader!", me, need)
				rf.mu.Lock()
				if rf.state == CandidateState && rf.currentTerm == args.Term {
					rf.state = LeaderState
					rf.currentTerm++
					// 2B: initializeLogs

					// start heartbeat
					go rf.raiseHearbeat(rf.currentTerm)
				} else {
					PrettyDebug(dWarn, "S%d is no longer a candidate or in the new Term:%d, cannot raise to a leader", me, rf.currentTerm)
				}
				rf.mu.Unlock()
			}
		} else if reply.Term > args.Term {
			PrettyDebug(dCandidate, "S%d receive higher term(%d > %d) from S%d, set back to Follower", me, reply.Term, args.Term, peer)
			rf.mu.Lock()
			if rf.state == CandidateState && rf.currentTerm == args.Term {
				rf.state = FollowerState
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				cnt.Clear()
			} else {
				PrettyDebug(dWarn, "S%d is not a candidate or in the new Term:%d, this won't affect", me, rf.currentTerm)
			}
			rf.mu.Unlock()
		}
	}
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
	rf.heartbeaten()

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]Entry, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
