package raft

import "math"

type Entry struct {
	Term    int
	Command interface{}
}

// index = lastIncludedIndex + i
func (rf *Raft) getLog(index int) *Entry {
	// if index == 0 {
	// 	return &rf.logs[index]
	// }
	i := index - rf.lastIncludedIndex
	return &rf.logs[i]
}

func (rf *Raft) getLogs(startIndex, endIndex int) []Entry {
	slice := []Entry(nil)
	for i := startIndex; i < endIndex; i++ {
		slice = append(slice, rf.logs[i-rf.lastIncludedIndex])
	}
	return slice
}

func (rf *Raft) copyLogs(startIndex, endIndex int) []Entry {
	slice := []Entry(nil)
	slice = append(slice, Entry{0, nil})
	for i := startIndex; i < endIndex; i++ {
		slice = append(slice, rf.logs[i-rf.lastIncludedIndex])
	}
	return slice
}

// Need Lock
// return last's index and term
// index = 0 means no logs
func (rf *Raft) getLastLogInfo() (index int, term int) {
	if len(rf.logs) <= 1 {
		return rf.lastIncludedIndex, rf.lastIncludedTerm
	}
	lastLogIndex := len(rf.logs) - 1 + rf.lastIncludedIndex
	lastLogTerm := rf.logs[len(rf.logs)-1].Term
	return lastLogIndex, lastLogTerm
}

func (rf *Raft) getLogIndexesWithTerm(term int) (firstIdx, lastIdx int) {
	if term == 0 {
		return 0, 0
	}
	first, last := math.MaxInt, -1
	if rf.lastIncludedTerm == term {
		first = min(first, rf.lastIncludedIndex)
		last = max(last, rf.lastIncludedIndex)
	}
	for i := 1; i < len(rf.logs); i++ {
		if rf.logs[i].Term == term {
			first = min(first, i+rf.lastIncludedIndex)
			last = max(last, i+rf.lastIncludedIndex)
		}
	}
	if last == -1 {
		return -1, -1
	}
	return first, last
}
