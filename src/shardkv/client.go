package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
	"6.5840/shardctrler"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk //用于获取或者更新配置
	config   shardctrler.Config //缓存最新的配置
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	seqId    int
	clientId int64
}

// MakeClerk
// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardctrler.MakeClerk().
//
// make_end(servername) turns a server name from a
// UpConfig.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.seqId = 0
	ck.config = ck.sm.Query(-1)
	return ck
}

// Get GetType
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (ck *Clerk) Get(key string) string {
	ck.seqId++

	for {
		args := GetArgs{
			Key:       key,
			ClientId:  ck.clientId,
			RequestId: ck.seqId,
		}
		shard := key2shard(key)                       //根据key计算出shard
		gid := ck.config.Shards[shard]                //根据shard计算出gid
		if servers, ok := ck.config.Groups[gid]; ok { //根据gid获取对应的servers
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				if ok {
				} else {
				}

				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
			}
		}

		//如果没有return,则说明发生了错误,需要重新获取最新的配置
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1) //更新最新的配置
	}

}

// PutAppend
// shared by Put and Append.
// You will have to modify this function.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.seqId++

	for {
		args := PutAppendArgs{
			Key:       key,
			Value:     value,
			Op:        Operation(op),
			ClientId:  ck.clientId,
			RequestId: ck.seqId,
		}
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				if ok {
				} else {
				}
				if ok && reply.Err == OK {
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
			}
		}

		//如果没有return,则说明发生了错误,需要重新获取最新的配置
		time.Sleep(100 * time.Millisecond)
		//重新获取最新的配置
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
