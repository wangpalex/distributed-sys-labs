package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)

const AGREE_TIMEOUT = 500

type OpType string

const (
	OpGet    = "OpGet"
	OpPut    = "OpPut"
	OpAppend = "OpAppend"
)

type Op struct {
	Type     OpType
	Key      string
	Value    string
	ClientId int64
	SeqNum   int64
}

type Result struct {
	Value string
	Err   Err
}

type KVServer struct {
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // Set by Kill()

	persister    *raft.Persister
	maxraftstate int // Take snapshot if log size exceeds threshold

	lastApplied int
	lastSeq     map[int64]int64 // Last seqNum of each client
	db          map[string]string
	notifyChs   map[int]chan Result // Fan-out applyCh to notifyChs by log index
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
	// Call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.persister = persister
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.lastApplied = 0
	kv.lastSeq = make(map[int64]int64)
	kv.db = make(map[string]string)
	kv.notifyChs = make(map[int]chan Result)
	kv.decodeStates(persister.ReadSnapshot())
	go kv.notifier()

	return kv
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Type:     OpGet,
		Key:      args.Key,
		ClientId: args.ClientId,
		SeqNum:   -1,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	Debug(dServer, "Server %d: Handling Get RPC, args=%+v", kv.me, *args)
	ch := kv.getNotifyCh(index)
	select {
	case res := <-ch:
		reply.Value, reply.Err = res.Value, res.Err
		Debug(dServer, "Server %d: Replied Get RPC, reply=%+v", kv.me, *reply)
	case <-time.After(AGREE_TIMEOUT * time.Millisecond):
		Debug(dServer, "Server %d: Get timeout", kv.me)
		reply.Value, reply.Err = "", ErrTimeout
	}

	// Async delete the notify channel
	go kv.removeNotifyCh(index)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if kv.isDuplicate(args.ClientId, args.SeqNum) {
		reply.Err = OK
		return
	}

	op := Op{
		Type:     args.Type,
		Key:      args.Key,
		Value:    args.Value,
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	Debug(dServer, "Server %d: Handling PutAppend RPC, args=%+v", kv.me, *args)
	ch := kv.getNotifyCh(index)
	select {
	case res := <-ch:
		Debug(dServer, "Server %d: PutAppend success, args=%+v", kv.me, *args)
		reply.Err = res.Err
	case <-time.After(AGREE_TIMEOUT * time.Millisecond):
		Debug(dServer, "Server %d: PutAppend timeout, args=%+v", kv.me, *args)
		reply.Err = ErrTimeout
	}

	// Async delete the notify channel
	go kv.removeNotifyCh(index)
}

func (kv *KVServer) notifier() {
	for !kv.killed() {
		for msg := range kv.applyCh {
			if msg.CommandValid {
				op := msg.Command.(Op)
				res := kv.applyOp(op, msg.CommandIndex)

				if currTerm, isLeader := kv.rf.GetState(); isLeader && msg.CommandTerm == currTerm {
					// Notify only if is leader and the term matches
					ch := kv.getNotifyCh(msg.CommandIndex)
					ch <- res
				}
			} else if msg.SnapshotValid {
				kv.applySnapshot(msg.Snapshot, msg.SnapshotIndex)
			}
		}
	}
}

func (kv *KVServer) applyOp(op Op, commandIndex int) Result {
	// This func is synchronous in notifier(), no need to lock
	res := Result{}
	res.Err = OK
	if commandIndex <= kv.lastApplied {
		return res
	}

	if op.Type == OpGet {
		Debug(dApply, "Server %d: Applied Get, index=%v, Op=%+v", kv.me, commandIndex, op)
		if val, ok := kv.db[op.Key]; ok {
			res.Value = val
		} else {
			res.Value, res.Err = "", ErrNoKey
		}
	} else if !kv.isDuplicate(op.ClientId, op.SeqNum) {
		if op.Type == OpPut {
			Debug(dApply, "Server %d: Applied Put, index=%v, Op=%+v", kv.me, commandIndex, op)
			kv.db[op.Key] = op.Value
		} else if op.Type == OpAppend {
			Debug(dApply, "Server %d: Applied Append, index=%v, Op=%+v", kv.me, commandIndex, op)
			kv.db[op.Key] += op.Value
		}
		kv.updateMaxSeq(op.ClientId, op.SeqNum)

		Debug(dServer, "Server %v: DB after apply PutAppend: %+v", kv.me, kv.db)
	}
	kv.lastApplied = commandIndex

	if kv.maxraftstate != -1 && kv.persister.RaftStateSize() >= kv.maxraftstate {
		data := kv.encodeStates()
		kv.rf.Snapshot(kv.lastApplied, data)
	}

	return res
}

func (kv *KVServer) applySnapshot(data []byte, snapshotIndex int) {
	kv.decodeStates(data)
	kv.lastApplied = snapshotIndex
	Debug(dApply, "Server %d: Applied Snapshot, index=%d", kv.me, snapshotIndex)
	Debug(dServer, "Server %v: DB after apply Snapshot: %+v", kv.me, kv.db)
}

func (kv *KVServer) getNotifyCh(index int) chan Result {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.notifyChs[index]; !ok {
		kv.notifyChs[index] = make(chan Result)
	}
	return kv.notifyChs[index]
}

func (kv *KVServer) removeNotifyCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.notifyChs, index)
}

func (kv *KVServer) isDuplicate(clientId int64, seqNum int64) bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return seqNum <= kv.lastSeq[clientId]
}

func (kv *KVServer) updateMaxSeq(clientId int64, seqNum int64) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.lastSeq[clientId] = seqNum
}

func (kv *KVServer) encodeStates() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)

	kv.mu.RLock()
	e.Encode(kv.lastSeq)
	kv.mu.RUnlock()

	return w.Bytes()
}

func (kv *KVServer) decodeStates(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var lastSeq map[int64]int64

	if d.Decode(&db) != nil {
		Debug(dServer, "Server %d: Error decoding DB states", kv.me)
	} else {
		kv.db = db
	}

	kv.mu.Lock()
	if d.Decode(&lastSeq) != nil {
		Debug(dServer, "Server %d: Error decoding DB states", kv.me)
	} else {
		kv.lastSeq = lastSeq
	}
	kv.mu.Unlock()
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
