package kvraft

import "6.5840/labrpc"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.

	id       int64
	seqNum   int64
	leaderId int
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
	ck.id = nrand()
	ck.seqNum = 1
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
	args := GetArgs{Key: key, ClientId: ck.id}
	leader := ck.leaderId
	defer func() {
		ck.leaderId = leader
	}()

	for {
		reply := GetReply{}
		Debug(dClient, "Client %d: Sending Get request, args=%+v", ck.id, args)
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			Debug(dClient, "Client %d: Cannot reach server, err=%v", ck.id, reply.Err)
			leader = (leader + 1) % len(ck.servers)
			continue
		}

		Debug(dClient, "Client %d: Get request success, reply=%+v", ck.id, reply)
		if reply.Err == ErrNoKey {
			return ""
		}
		return reply.Value
	}
}

// shared by OpPut and OpAppend.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, opType OpType) {
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Type:     opType,
		ClientId: ck.id,
		SeqNum:   ck.seqNum,
	}
	leader := ck.leaderId
	defer func() {
		ck.seqNum++
		ck.leaderId = leader
	}()

	for {
		reply := PutAppendReply{}
		Debug(dClient, "Client %d: Sending PutAppend request, args=%+v", ck.id, args)
		ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply)
		if !ok || reply.Err == ErrWrongLeader || reply.Err == ErrTimeout {
			Debug(dClient, "Client %d: Cannot reach server, err=%v", ck.id, reply.Err)
			leader = (leader + 1) % len(ck.servers)
			continue
		}

		Debug(dClient, "Client %d: Seq %d PutAppend request success", ck.id, args.SeqNum)
		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, OpPut)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, OpAppend)
}
