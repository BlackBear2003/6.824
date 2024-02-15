package kvraft

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongLeader = "ErrWrongLeader"
	ErrTimeout     = "ErrTimeout"
)

type Err string

type ExecArgs struct {
	Key       string
	Value     string
	Op        string // "Put" or "Append" or "Get"
	CommandId int64
	ClientId  int64
}

type ExecReply struct {
	Err   Err
	Value string
}
