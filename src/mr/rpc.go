package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

type CallForAssignTaskArgs struct {
	WorkerId string
}

type CallForAssignTaskReply struct {
	T *Task
}

type CallForSubmitTaskArgs struct {
	WorkerId string
	Commits  []CommitFile
	T        *Task
}

type CallForSubmitTaskReply struct {
}

type CommitFile struct {
	RealFilename string
	TmpFilename  string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
