package kvsrv

import (
	"crypto/rand"
	"math/big"

	"6.5840/labrpc"
)

type Clerk struct {
	server   *labrpc.ClientEnd
	clientID int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.clientID = nrand() // 生成一个随机的客户端ID
	return ck
}

func (ck *Clerk) Get(key string) string {
	args := &GetArgs{
		Key: key,
	}
	reply := &GetReply{}
	for !ck.server.Call("KVServer.Get", args, reply) {
	} // keep trying forever
	return reply.Value
}

func (ck *Clerk) PutAppend(key string, value string, op string) string {
	MessageID := nrand()
	arg := &PutAppendArgs{
		Key:         key,
		Value:       value,
		MessageID:   MessageID,
		MessageType: Modify,
	}
	reply := &PutAppendReply{}
	for !ck.server.Call("KVServer."+op, arg, reply) {
	}
	arg = &PutAppendArgs{
		MessageType: Report,
		MessageID:   MessageID,
	}
	for !ck.server.Call("KVServer."+op, arg, reply) {
	}
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
