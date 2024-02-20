package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId  int64
	leaderId  int
	commandId int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.leaderId = 0
	ck.commandId = 1

	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	return ck.exec(&ExecArgs{Key: key, Op: "Get"})
}

// shared by Put and Append and Get
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
func (ck *Clerk) exec(args *ExecArgs) string {
	args.ClientId = ck.clientId
	args.CommandId = ck.commandId // atomic.AddInt64(&, 1)
	for {
		reply := &ExecReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Exec", args, reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		ck.commandId++
		return reply.Value
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.exec(&ExecArgs{Key: key, Value: value, Op: "Put"})
}
func (ck *Clerk) Append(key string, value string) {
	ck.exec(&ExecArgs{Key: key, Value: value, Op: "Append"})
}
