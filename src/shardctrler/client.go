package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"math/big"
	mathrand "math/rand"
	"time"

	"6.5840/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	seqId    int
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

	// Your code here.
	ck.clientId = nrand()
	ck.leaderId = mathrand.Intn(len(ck.servers))
	return ck
}

func (ck *Clerk) Query(num int) Config {

	// Your code here.
	ck.seqId++
	args := QueryArgs{Num: num, ClientId: ck.clientId, SeqId: ck.seqId}
	serverId := ck.leaderId
	for {
		reply := QueryReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Query", &args, &reply)

		if ok {
			if reply.Err == OK {
				ck.leaderId = serverId
				return reply.Config
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}

		// 节点发生crash等原因
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}

}

func (ck *Clerk) Join(servers map[int][]string) {
	// Your code here.
	ck.seqId++
	args := JoinArgs{Servers: servers, ClientId: ck.clientId, SeqId: ck.seqId}
	serverId := ck.leaderId
	for {
		reply := JoinReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Join", &args, &reply)

		if ok {
			if reply.Err == OK {
				ck.leaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}

}

func (ck *Clerk) Leave(gids []int) {
	ck.seqId++
	args := LeaveArgs{GIDs: gids, ClientId: ck.clientId, SeqId: ck.seqId}
	serverId := ck.leaderId
	for {
		reply := JoinReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Leave", &args, &reply)

		if ok {
			if reply.Err == OK {
				ck.leaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}

		// 节点发生crash等原因
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.seqId++
	args := MoveArgs{Shard: shard, GID: gid, ClientId: ck.clientId, SeqId: ck.seqId}
	serverId := ck.leaderId
	for {
		reply := JoinReply{}
		ok := ck.servers[serverId].Call("ShardCtrler.Move", &args, &reply)

		if ok {
			if reply.Err == OK {
				ck.leaderId = serverId
				return
			} else if reply.Err == ErrWrongLeader {
				serverId = (serverId + 1) % len(ck.servers)
				continue
			}
		}
		serverId = (serverId + 1) % len(ck.servers)
		time.Sleep(100 * time.Millisecond)
	}
}
