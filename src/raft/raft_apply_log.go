package raft

import "time"

func (rf *Raft) applyLogHandler(applyCh chan ApplyMsg) {
	for !rf.killed() {
		time.Sleep(10 * time.Millisecond)

		appliedLogs := []ApplyMsg{}
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			applied := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			}
			appliedLogs = append(appliedLogs, applied)
			PrettyDebug(dClient, "S%d applied log of index:%d(Term:%d)", rf.me, rf.lastApplied, rf.logs[rf.lastApplied].Term)
		}
		rf.mu.Unlock()
		for _, a := range appliedLogs {
			applyCh <- a
		}
	}
}
