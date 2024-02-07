package raft

import (
	"time"
)

func (rf *Raft) applyLogHandler(applyCh chan ApplyMsg) {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		appliedMsgs := []ApplyMsg{}
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			if rf.lastApplied <= rf.lastIncludedIndex {
				continue
			}
			PrettyDebug(dClient, "S%d applying log of index:%d{%d %v}", rf.me, rf.lastApplied, rf.getLog(rf.lastApplied).Term, rf.getLog(rf.lastApplied).Command)
			applied := ApplyMsg{
				CommandValid: true,
				Command:      rf.getLog(rf.lastApplied).Command,
				CommandIndex: rf.lastApplied,
			}
			appliedMsgs = append(appliedMsgs, applied)
		}
		rf.mu.Unlock()

		for _, msg := range appliedMsgs {
			applyCh <- msg
		}
	}
}
