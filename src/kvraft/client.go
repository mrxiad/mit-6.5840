package kvraft

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"

	mathrand "math/rand"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	seqId    int // 防止重复消费消息
	leaderId int // 确定哪个服务器是leader，下次直接发送给该服务器
	clientId int64
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
	ck.leaderId = mathrand.Intn(len(ck.servers)) //随机一个leader
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.seqId++
	serverId := ck.leaderId
	args := GetArgs{Key: key, ClientId: ck.clientId, SeqId: ck.seqId}
	for {

		reply := GetReply{}
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)

		if ok {
			if reply.Err == ErrNoKey {
				ck.leaderId = serverId
				return ""
			} else if reply.Err == OK {
				ck.leaderId = serverId
				return reply.Value
			} else if reply.Err == ErrWrongLeader { //换leader
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}

		// 节点发生crash等原因
		serverId = (serverId + 1) % len(ck.servers) //换leader

	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.seqId++
	serverId := ck.leaderId
	args := PutAppendArgs{Key: key, Value: value, Op: op, ClientId: ck.clientId, SeqId: ck.seqId}
	for {

		reply := PutAppendReply{}
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		if ok {
			if reply.Err == OK {
				ck.leaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader { //换leader
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}

		serverId = (serverId + 1) % len(ck.servers) //换leader

	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
