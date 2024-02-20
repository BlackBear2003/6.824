package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false
const ExecuteTimeout = 200 * time.Millisecond

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	OpType    string // "Put" or "Append" or "Get"
	CommandId int64
	ClientId  int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	lastApplied int // record the lastApplied to prevent stateMachine from rollback
	kvService   KvService
	cmdMap      map[int64]CommandCtx    // (clientId, 最后的commandId)
	notifyChans map[int]chan *ExecReply // notify client goroutine by applier goroutine to response
	persister   *raft.Persister
}

type CommandCtx struct {
	CommandId int64
	Reply     *ExecReply
}

func (kv *KVServer) isDupliceCommand(clientId, commandId int64) bool {
	return kv.cmdMap[clientId].CommandId >= commandId
}

func (kv *KVServer) getLastCommandReply(clientId int64) *ExecReply {
	return kv.cmdMap[clientId].Reply
}

func (kv *KVServer) setLastCommandReply(clientId, commandId int64, reply *ExecReply) {
	kv.cmdMap[clientId] = CommandCtx{CommandId: commandId, Reply: reply}
}

// need Lock
func (kv *KVServer) registerNotifyChan(index int) chan *ExecReply {
	// 检查给定索引对应的通道是否已经存在
	kv.notifyChans[index] = make(chan *ExecReply, 1)
	return kv.notifyChans[index]
}

func (kv *KVServer) getNotifyChan(index int) chan *ExecReply {
	return kv.notifyChans[index]
}

func (kv *KVServer) hasNotifyChan(index int) bool {
	_, ok := kv.notifyChans[index]
	return ok
}

func (kv *KVServer) deleteNotifyChan(index int) {
	delete(kv.notifyChans, index)
}

func (kv *KVServer) Exec(args *ExecArgs, reply *ExecReply) {
	// Your code here.
	kv.mu.Lock()
	if args.Op != "Get" && kv.isDupliceCommand(args.ClientId, args.CommandId) {
		// 收到已经执行过的cmd
		r := kv.getLastCommandReply(args.ClientId)
		reply.Value, reply.Err = r.Value, r.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	index, _, isLeader := kv.rf.Start(Op{Key: args.Key, Value: args.Value, OpType: args.Op, ClientId: args.ClientId, CommandId: args.CommandId})
	if !isLeader {
		reply.Value = ""
		reply.Err = ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := kv.registerNotifyChan(index)
	kv.mu.Unlock()
	// 阻塞到收到消息
	select {
	case recv := <-ch:
		reply.Value, reply.Err = recv.Value, recv.Err
	case <-time.After(ExecuteTimeout):
		reply.Value = ""
		reply.Err = ErrTimeout
	}
	// channel已经outdate了，可以删掉
	go func(index int) {
		kv.mu.Lock()
		kv.deleteNotifyChan(index)
		kv.mu.Unlock()
	}(index)
}

func (kv *KVServer) notifier() {
	for !kv.killed() {
		select {
		case msg := <-kv.applyCh:
			if msg.CommandValid {
				kv.mu.Lock()
				if msg.CommandIndex <= kv.lastApplied {
					// discards outdated message, maybe snapshot
					kv.mu.Unlock()
					continue
				}
				var reply *ExecReply
				if _, ok := msg.Command.(Op); !ok {
					kv.mu.Unlock()
					continue
				}
				kv.lastApplied = msg.CommandIndex
				cmd := msg.Command.(Op)
				if cmd.OpType != "Get" && kv.isDupliceCommand(cmd.ClientId, cmd.CommandId) {
					// doesn't apply duplicated message to stateMachine
					reply = kv.getLastCommandReply(cmd.ClientId)
				} else {
					switch cmd.OpType {
					case "Put":
						kv.kvService.put(cmd.Key, cmd.Value)
						reply = &ExecReply{OK, ""}
					case "Append":
						kv.kvService.append(cmd.Key, cmd.Value)
						reply = &ExecReply{OK, ""}
					case "Get":
						if value, err := kv.kvService.get(cmd.Key); err == OK {
							reply = &ExecReply{OK, value}
						} else {
							reply = &ExecReply{err, ""}
						}
					}
					if cmd.OpType != "Get" {
						kv.setLastCommandReply(cmd.ClientId, cmd.CommandId, reply)
					}
				}

				// only notify related channel for currentTerm's log when node is leader
				if _, isLeader := kv.rf.GetState(); isLeader {
					if kv.hasNotifyChan(msg.CommandIndex) {
						ch := kv.getNotifyChan(msg.CommandIndex)
						ch <- reply
					}
				}

				if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
					kv.saveSnapshot(msg.CommandIndex)
				}
				kv.mu.Unlock()
			} else if msg.SnapshotValid {
				kv.mu.Lock()
				if msg.SnapshotIndex > kv.lastApplied {
					kv.installSnapshot(msg.Snapshot)
					kv.lastApplied = msg.SnapshotIndex
				}
				kv.mu.Unlock()
			} else {
				panic(fmt.Sprintf("unexpected Message %v", msg))
			}
		}
	}
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(KvService{})
	labgob.Register(CommandCtx{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.persister = persister
	kv.kvService = NewKvService()
	kv.lastApplied = 0
	kv.cmdMap = make(map[int64]CommandCtx)
	kv.notifyChans = make(map[int]chan *ExecReply)

	kv.installSnapshot(kv.persister.ReadSnapshot())

	go kv.notifier()

	return kv
}

// 保存快照
func (kv *KVServer) saveSnapshot(snapshotIndex int) {
	DPrintf("Snapshot: server: %v snapshotIndex:%v", kv.me, snapshotIndex)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.kvService)
	e.Encode(kv.cmdMap)
	e.Encode(kv.lastApplied)
	data := w.Bytes()
	kv.rf.Snapshot(snapshotIndex, data)
}

// 安装快照
func (kv *KVServer) installSnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var kvService KvService
	var cmdMap map[int64]CommandCtx
	var lastApplied int

	if d.Decode(&kvService) != nil || d.Decode(&cmdMap) != nil ||
		d.Decode(&lastApplied) != nil {
		DPrintf("error to read the snapshot data")
	} else {
		kv.kvService = kvService
		kv.cmdMap = cmdMap
		kv.lastApplied = lastApplied
	}
}
