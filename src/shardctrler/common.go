package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch UpConfig # num, or latest config if num==-1.
//
// A UpConfig (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. UpConfig
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// Config A configuration -- an assignment of shards to groups.
// Please don't change this.
// 最多有10个分片
type Config struct {
	/*
		根据key2shard函数,找到对应的shard
		然后根据shard找到对应的gid
		最后根据gid找到对应的servers
	*/
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

type JoinArgs struct {
	Servers  map[int][]string // new GID -> servers mappings
	SeqId    int
	ClientId int64
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs     []int
	SeqId    int
	ClientId int64
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard    int
	GID      int
	SeqId    int
	ClientId int64
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num      int // desired config number
	SeqId    int
	ClientId int64
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
