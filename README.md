# 关键struct

## Raft

```go
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int        // 当前的任期
	votedFor    int        // 投给了谁
	log         []logEntry // 日志条目数组

	commitIndex int // 已被提交的日志索引（由matchIndex决定，保证大多数节点提交的最大commitIndex）
	lastApplied int // 已经被应用到状态机的日志索引（lastApplied<=commitIndex)

	// 仅leader使用
	//matchIndex[i] + 1 <= nextIndex[i]
	nextIndex  []int // 领导者计划给每个server发送的日志索引
	matchIndex []int // 每个server已经commit的日志索引

	// 快照状态

	state     int   // follower/candidate/leader
	timer     Timer //计时器
	voteCount int   // 票数

	applyChan chan ApplyMsg
}
```



## RequestVoteArgs

```go
type RequestVoteArgs struct {
	Term         int // 候选人的任期
	CandidateId  int // 候选人ID
	LastLogIndex int // 候选人最后的日志索引
	LastLogTerm  int // 候选人最后的日志任期
}
```



## RequestVoteReply

```go
type RequestVoteReply struct {
	Term        int  // 投票人的当前任期
	VoteGranted bool // true表示该节点把票投给了候选人
}
```



## AppendEntriesArgs

```go
type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int
	PrevLogIndex int        // leader认为follower已经复制到了此位置
	PrevLogTerm  int        // 位于 PrevLogIndex 的日志条目的任期号
	Entries      []logEntry //需要被复制的日志条目的集合。在心跳消息中，这个数组可能为空，表示没有新的日志条目需要被复制，仅仅是为了维护心跳和领导地位。

	LeaderCommit int //leader的commitIndex
}
```



## AppendEntriesReply

```go
type AppendEntriesReply struct {
	Term        int  // leader's term
	Success     bool // 如果为true，则说明leader可能更新commitIndex
	CommitIndex int  // follower通知leader自己的CommitIndex信息，更新leader的nextIndex[i]
}
```





# 状态转换

### follower变为candidate

如果定时器超时，则follower变为candidate，并且增加Term，发起投票选举。

### candidate变为leader

如果获取到大多数选票，则称为leader

### leader变为follower

如果其他server的term比自己大，则变为follower



## 一些恶心的情况

### 情况1:某个leader被网络分区，其他follower变为candidate当选leader，此时老leader网络恢复

- 老leader的Term会更小，但是日志会更多
- 网络分区期间，老leader的CommitIndex不会增加（因为没有将日志复制给大多数节点）
- candidate会收到更多的选票（前提是他的日志不旧）变成新leader，集群server的Term变大
- 老leader网络恢复后，发送心跳，发现自己的Term更小，变为follower



### 情况2:某个follower被网络分区变成candidate，网络恢复后发起投票

- 某个follower被网络分区后，变成candidate，由于收不到选票，则**无限增加Term**
- 网络恢复后，这个candidate的**Term更大**，**日志更旧**
- 老leader发送**心跳**给这个candidate，则自己会变成follower么，Term变大。
- 或者此candidate发送**投票请求**给其他server，其他server的Term会变大，并且变成follower，但candidate不会收到选票
- 最终集群中**拥有最新日志的candidate当选leader**



### 情况3:leader增大自身的CommitIndex，但没来得及发心跳，自己被网络分区，等到新leader当选，老leader网络恢复

- 前提：如果leader可以增大CommitIndex，则leader的日志已经被复制到了大多数节点上
- 当**拥有最新日志**的candidate变为leader后，将自身的**nextIndex**全都变为**自身日志长度**
- 新leader发送心跳到老leader（如果老leader网络恢复，因为Term小，变为follower），触发一致性检查
- 一致性检查中，老leader会**丢弃掉**自己**在网络分区中**获取到的日志



# 日志

## 特性

- 如果两个不同日志中的条目具有相同的索引和任期，则它们存储相同的命令
- 如果两个不同日志中的条目具有相同的索引和任期，则这两个日志在所有前面的日志中都相同

## 新

- 如果日志的最后一条记录具有不同的任期，则任期较晚的日志更最新
- 如果日志以相同的任期结尾，则较长的日志更新程度更高。





# 关键变量

## CommitIndex

### 含义

该server已经提交了的日志索引

### 特性

1. 每一个server的CommitIndex**只增不减**

2. 集群中，follower的CommitIndex可能会大于leader的

   > 原因：老leader根据MatchIndex发现CommitIndex可以增加，但是发送心跳同步follower的时候突然掉线，那么follower会当选新leader，这个时候，老leader恢复，发送心跳发现自己的Term更小，变成follower，此时follower的CommitIndex>leader的CommitIndex

3. 只有当绝大多数server认为可以提交的时候，leader才会提交，并且







## LastApplied

### 含义

该server应用到状态机的日志索引，LastApplied<=CommitIndex，正常情况下是相等

> LastApplied一直追赶CommitIndex，引用到状态机上，一般不深入考虑



## NextIndex

### 含义

leader认为自己应该给每个follower发送的日志索引，允许不正确，在一致性检查的时候会被更新为对的。如果一致性检查正确，此变量和MatchIndex同步更新

### 特性

1. 如果一致性检查正确，此变量和MatchIndex同步更新
2. 直接初始化较大，一致性检查的时候会减小，如果初始化较小，也无所谓



## MatchIndex

### 含义

leader认为每个follower已经commit的日志索引，一般和NextIndex同步更新

### 特性

1. 只有当大多数server可以提交（但没提交）日志a，leader才可以让自己的CommitIndex变成a，然后发出心跳
2. 如果follower正常同步日志，`MatchIndex[i] == NextIndex[i]-1 == len(follower.log)-1`
3. 一般是MatchIndex和NextIndex同步更新

 



# 关键流程

## 计时器

```go
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.timer.timer.C:
			//fmt.Printf("svr:%d,term:%d\n", rf.me, rf.currentTerm)
			rf.mu.Lock()
			switch rf.state {
			case Follower: // follower变candidate
				//fmt.Println("随从超时[id],[term]:", rf.me, rf.currentTerm)
				rf.state = Candidate
				fallthrough
			case Candidate: // 成为候选人，开始拉票
				rf.currentTerm++
				rf.voteCount = 1
				rf.timer.reset()
				rf.votedFor = rf.me

				// 开始拉票选举
				for i := 0; i < len(rf.peers); i++ {
					if rf.me == i { // 排除自己
						continue
					}
					args := RequestVoteArgs{
						Term:         rf.currentTerm,
						CandidateId:  rf.me,
						LastLogIndex: len(rf.log) - 1}

					if len(rf.log) > 0 {
						args.LastLogTerm = rf.log[len(rf.log)-1].Term
					}
					reply := RequestVoteReply{}
					go rf.sendRequestVote(i, &args, &reply)
				}
			case Leader:
				//fmt.Println("ld:", rf.me, "term:", rf.currentTerm)
				rf.timer.resetHeartBeat()
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					args := AppendEntriesArgs{
						Term:         rf.currentTerm,
						LeaderId:     rf.me,
						PrevLogIndex: rf.nextIndex[i] - 1,
						PrevLogTerm:  0,
						Entries:      nil,
						LeaderCommit: rf.commitIndex}

					if args.PrevLogIndex >= 0 {
						args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
					}

					//同步日志
					if rf.nextIndex[i] < len(rf.log) {
						entries := rf.log[rf.nextIndex[i]:]
						args.Entries = make([]logEntry, len(entries))
						copy(args.Entries, entries)
					}

					reply := AppendEntriesReply{}
					go rf.sendAppendEntries(i, &args, &reply)
				}

			}
			rf.mu.Unlock()
		}
	}
}
```



## 投票

```go
// 投票rpc
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm //投票人的当前任期
	reply.VoteGranted = false   //默认不投票

	// 1. 对方的term比自己小，不投票
	if args.Term < rf.currentTerm {
		return
	}

	//判断候选人的日志是否落后于自己
	var isLast = len(rf.log)-1 >= 0 &&
		(args.LastLogTerm < rf.log[len(rf.log)-1].Term ||
			args.LastLogTerm == rf.log[len(rf.log)-1].Term && args.LastLogIndex < len(rf.log)-1)
	// fmt.Printf("%d不给%d投票\n", rf.me, args.CandidateId)
	// fmt.Printf("候选人参数是:[%v,%v],自己是[%v,%v]\n",
	// 	args.LastLogTerm,
	// 	args.LastLogIndex,
	// 	rf.log[len(rf.log)-1].Term,
	// 	len(rf.log)-1)

	if isLast {
		if args.Term > rf.currentTerm { //如果对方任期比自己大，自己变为follower,不投票
			rf.convert2Follower(args.Term)
		}
		return
	}

	// 2. 对方的term比自己大，可能自己是candidate，自己则变为follower，但是否投票给对方取决于情况3
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.timer.reset() //重置时间
		rf.voteCount = 0
		rf.votedFor = args.CandidateId
	}

	// 3.自己还没投给别人（此时肯定是follower）
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true // 投票
		rf.timer.reset()         //重置时间
	}

}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	if ok := rf.peers[server].Call("Raft.RequestVote", args, reply); !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		return
	}

	// 如果对方选择投票给自己
	if reply.VoteGranted {
		rf.voteCount++
		if rf.voteCount >= (len(rf.peers)+1)/2 { // 获得大多数的选票
			rf.state = Leader //变为leader
			for i := 0; i < len(rf.nextIndex); i++ {
				rf.nextIndex[i] = len(rf.log)
				rf.matchIndex[i] = -1
			}
			rf.timer.resetHeartBeat() // 重制心跳时间
		}
	} else { //对方拒绝投票
	}
	//fmt.Printf("%d收到%d的投票%v?\n", rf.me, server, reply.VoteGranted)

	return
}
```



## 心跳

```go

// 收到心跳
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.CommitIndex = 0

	if args.Term < rf.currentTerm { //由于之前的leader被分区，强制之前的leader变为follower
		return
	}

	if args.Term >= rf.currentTerm { // 自己变为follower（之前可能是candidate）
		rf.convert2Follower(args.Term)
	}

	rf.timer.reset() //在convert2Follower有reset，但不一定保证能调用convert2Follower，所以需要手动reset

	if args.PrevLogIndex >= 0 && // leader要有日志
		(len(rf.log)-1 < args.PrevLogIndex || // 1. preLogIndex大于当前日志最大下标，说明缺失日志，拒绝附加
			rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) { // 2. 在相同index下日志term不同

		reply.CommitIndex = len(rf.log) - 1        //1.通知leader需要从这个index开始同步
		if reply.CommitIndex > args.PrevLogIndex { //2.相同index下的term不同，需要缩小一下范围
			reply.CommitIndex = args.PrevLogIndex
		}
		curTerm := rf.log[reply.CommitIndex].Term

		for reply.CommitIndex >= 0 {
			if rf.log[reply.CommitIndex].Term == curTerm { //todo：当前term可能不需要回退
				reply.CommitIndex--
			} else {
				break
			}
		}
		reply.Success = false // 返回false,此节点日志没有跟上leader，或者有多余日志，或者日志有冲突
		return
	} else if args.Entries == nil { //心跳包
		//fmt.Println("[id]接受到心跳", rf.me)
		if rf.lastApplied < args.LeaderCommit { //追赶日志
			rf.commitIndex = args.LeaderCommit
			go rf.applyLogs() // 提交日志
		}
		reply.CommitIndex = len(rf.log) - 1 // 用于leader更新nextIndex
		reply.Success = true
		return
	} else { //此时更新commitIndex,并复制日志
		rf.log = rf.log[:args.PrevLogIndex+1]    // 第一次调用的时候prevlogIndex为-1
		rf.log = append(rf.log, args.Entries...) // 将args中后面的日志一次性全部添加进log

		if rf.lastApplied < args.LeaderCommit { //如果日志还没追赶上，就追赶日志
			rf.commitIndex = args.LeaderCommit
			go rf.applyLogs()
		}

		// 用于leader更新nextIndex[i]和matchIndex[i]
		// reply.CommitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
		reply.CommitIndex = len(rf.log) - 1
		if args.LeaderCommit > rf.commitIndex && args.LeaderCommit < len(rf.log)-1 {
			reply.CommitIndex = args.LeaderCommit
		}
		return
	}
}

// 作为leader发送心跳
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if ok := rf.peers[server].Call("Raft.AppendEntries", args, reply); !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || rf.currentTerm != args.Term {
		return
	}

	if rf.currentTerm < reply.Term { // 1.自己的term没别人的大，变为follower
		rf.convert2Follower(reply.Term)
		return
	}

	if reply.Success { //可能更新commitIndex
		rf.nextIndex[server] = reply.CommitIndex + 1 // CommitIndex为对端确定两边相同的index 加上1就是下一个需要发送的日志
		rf.matchIndex[server] = reply.CommitIndex    //对方已经同步到这里了

		if rf.nextIndex[server] > len(rf.log) {
			rf.nextIndex[server] = len(rf.log)
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}

		//看是否可以更新自己的commitIndex
		commitCount := 1
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			if rf.matchIndex[i] >= rf.matchIndex[server] {
				commitCount++
			}
		}
		if commitCount >= len(rf.peers)/2+1 && // 超过一半的数量接收日志了
			rf.commitIndex < rf.matchIndex[server] && // 不能多次提交
			rf.log[rf.matchIndex[server]].Term == rf.currentTerm { //再次保证正确性
			rf.commitIndex = rf.matchIndex[server]
			//fmt.Printf("ld:%d不应该提交,count:%d,commitIndex:%d,\n", rf.me, commitCount, rf.commitIndex)
			go rf.applyLogs() // 提交日志
		}

	} else { //对方不同意提交，需要更改nextIndex
		rf.nextIndex[server] = reply.CommitIndex + 1
		if rf.nextIndex[server] > len(rf.log) {
			rf.nextIndex[server] = len(rf.log)
		}
	}
}
```
