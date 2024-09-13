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
- 一致性检查中，老leader会**丢弃掉**自己**在网络分区中**新添加的日志





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

leader认为每个follower可以commit的日志索引(**此时follower的commitIndex还没更新呢**)，一般和NextIndex同步更新

### 特性

1. 只有当大多数server可以提交（但没提交）日志a，leader才可以让自己的CommitIndex变成a，然后发出心跳
2. 如果follower正常同步日志，`MatchIndex[i] == NextIndex[i]-1 == len(follower.log)-1`
3. 一般是MatchIndex和NextIndex同步更新
4. 不能丢弃这个变量，因为最开始，`MatchIndex=0`，但是`NextIndex=len(log)`,如果**仅仅**用NextIndex去更新leader的commitIndex，就会出错

 



# 关键判断

## 选举

### 发送者

- 发送args，然后接收

- 判断对方任期是否大于自己，如果对方Term大，则cover2follower，return

- 否则判断自己是否可以变成leader，如果变成，进行初始化操作，并且重置定时器为心跳

- 如果已经是leader了，则直接退出，不要重复初始化

  

### 接受者

- 如果对方Term大，则更新自己为follower
- 如果对方日志旧，或者自己已经投票过别人了，则不投票
- 否则投票，并重置计时器



## 追加

### 发送者

- 如果能用快照更新，则发快照，return
- 根据`nextIndex[i]`决定发多少日志，如果日志为空，那么这条rpc就是心跳
- 接受响应后，将自身的nextIndex和matchIndex按照reply更新，并且判断能不能增加commitIndex



### 接受者

- 判断当前快照是否已经更新了，如果快照已经更新，则直接return即可
- 如果`prevLogIndex>LastIndex`，说明有日志缺失
- 如果索引上日志的Term不正确，需要回退一个Term
- 正确的话，直接覆盖之后的日志，并将自己的commitIndex更新为leaderCommitIndex



## 快照

### 发送者

- 对方日志差太多了，就发送一次快照
- 如果对方成功接受快照，则leader需要自己更新matchIndex以及nextIndex



### 接受者

- 不需要和commitIndex比较
- 将index之前的快照保存，并将之后的快照切割
- 更新commitIndex以及lastApplied
- 将信息持久化





# 持久化

每个server在以下三个变量变化的时候，进行持久化操作

```go
currentTerm
votedFor
log[]
```





# 快照

每个节点都会**存取自身的快照，快照的信息就相当于commit过后的日志。**

## 注意

1. peers自主更新时:`上一次快照index<目标快照index<=commitIndex`
2. leader主动推送快照时:按照leader快照更新即可