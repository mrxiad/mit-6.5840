package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labgob"
	"6.5840/labrpc"
)

const (
	Follower  = iota //0
	Candidate        //1
	Leader           //2
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type logEntry struct {
	Command interface{} // 当前命令的请求
	Term    int         // 当前日志的任期
}

type Timer struct {
	timer *time.Ticker
}

func (t *Timer) reset() {
	randomTime := time.Duration(150+rand.Intn(200)) * time.Millisecond // 150~350ms
	t.timer.Reset(randomTime)
}

func (t *Timer) resetHeartBeat() { //心跳时间
	t.timer.Reset(80 * time.Millisecond)
}

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

	// 用于快照
	lastIncludedIndex int //最后一个包含的快照的日志条目的索引
	lastIncludedTerm  int //最后一个包含的快照的日志条目的任期
}

// (Term,IsLeader)
func (rf *Raft) GetState() (int, bool) {
	var isleader = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		isleader = true
	}
	return rf.currentTerm, isleader
}

// 非易失性数据（保存）
func (rf *Raft) persistData() []byte {
	var err error
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err = e.Encode(rf.currentTerm)
	err = e.Encode(rf.votedFor)
	err = e.Encode(rf.log)
	err = e.Encode(rf.lastIncludedIndex)
	err = e.Encode(rf.lastIncludedTerm)
	if err != nil {
		panic(err)
	}
	return w.Bytes()
}

func (rf *Raft) persist() {
	raftstate := rf.persistData()
	if raftstate == nil {
		panic("encode 失败!")
	}
	rf.persister.SaveRaftState(raftstate)
}

// 恢复raft状态,RaftState & Snapshot
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		currentTerm      int
		votedFor         int
		log              []logEntry
		lastIncludeIndex int
		lastIncludeTerm  int
	)
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
		panic("decode error!")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludeIndex
		rf.lastIncludedTerm = lastIncludeTerm
	}
}

// 外部调用,每一个server都可以保存,想保存快照的前提是已经被commit
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果下标大于自身commit，说明没被提交，不能执行快照，若自身快照大于index则说明已经执行过快照，也不需要
	if rf.lastIncludedIndex >= index || index > rf.commitIndex {
		return
	}

	// 裁剪日志
	sLog := []logEntry{}
	for i := index + 1; i <= rf.getLastIndex(); i++ {
		sLog = append(sLog, rf.restoreLog(i))
	}

	// 更新快照
	if index == rf.getLastIndex() {
		rf.lastIncludedTerm = rf.getLastTerm()
	} else {
		rf.lastIncludedTerm = rf.restoreLogTerm(index)
	}

	//更新快照
	rf.lastIncludedIndex = index
	rf.log = sLog

	// 重置commitIndex、lastApplied下标
	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot) //todo:需要重构,
}

type RequestVoteArgs struct {
	Term         int // 候选人的任期
	CandidateId  int // 候选人ID
	LastLogIndex int // 候选人最后的日志索引
	LastLogTerm  int // 候选人最后的日志任期
}

type RequestVoteReply struct {
	Term        int  // 投票人的当前任期
	VoteGranted bool // true表示该节点把票投给了候选人
}

type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int
	PrevLogIndex int        // leader认为follower已经复制到了此位置，可能不正确
	PrevLogTerm  int        // 位于 PrevLogIndex 的日志条目的任期号
	Entries      []logEntry //需要被复制的日志条目的集合。在心跳消息中，这个数组可能为空，表示没有新的日志条目需要被复制，仅仅是为了维护心跳和领导地位。

	LeaderCommit int //leader的commitIndex
}

type AppendEntriesReply struct {
	Term        int  // 接收者 term
	Success     bool // 如果为true，则说明leader可能更新commitIndex
	CommitIndex int  // follower通知leader自己的CommitIndex信息，更新leader的nextIndex[i]
}

type InstallSnapshotArgs struct {
	Term              int    // 发送请求的Term
	LeaderId          int    // 请求方的Id
	LastIncludedIndex int    // 快照最后applied的日志下标
	LastIncludedTerm  int    // 快照最后applied时的Term
	Data              []byte // 快照区块的原始字节流数据
	// offset				int		// 次传输chunk在快照文件的偏移量，快照文件可能很大，因此需要分chunk，此次不分片
	// Done 				bool	// true表示是最后一个chunk
}

type InstallSnapshotReply struct {
	Term int // 让leader自己更新的
}

// leader发送快照
func (rf *Raft) leaderSendSnapShot(server int) {
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	if ok := rf.sendSnapShot(server, &args, &reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || rf.currentTerm != args.Term { // 1.自己的状态改变了
		return
	}
	if reply.Term > rf.currentTerm { //自己变成follower
		rf.convert2Follower(reply.Term)
		rf.persist()
		return
	}

	//下次从这里更新
	rf.matchIndex[server] = args.LastIncludedIndex
	rf.nextIndex[server] = rf.matchIndex[server] + 1
}

// follower更新快照
func (rf *Raft) InstallSnapShot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.currentTerm > args.Term { //让对方变成follower
		reply.Term = rf.currentTerm
		return
	}
	reply.Term = args.Term
	rf.convert2Follower(args.Term)
	rf.persist()
	// 已经覆盖了这个快照了，不用再写入快照
	if rf.lastIncludedIndex >= args.LastIncludedIndex {
		return
	}

	// 将快照后的log切割，快照前的提交
	index := args.LastIncludedIndex
	rf.log = []logEntry{}

	for i := index + 1; i <= rf.getLastIndex(); i++ {
		rf.log = append(rf.log, rf.restoreLog(i))
	}

	//表示在index之前的日志都已经应用到状态机了
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = args.LastIncludedTerm

	if index > rf.commitIndex {
		rf.commitIndex = index
	}
	if index > rf.lastApplied {
		rf.lastApplied = index
	}

	applyMsg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  rf.lastIncludedTerm,
		SnapshotIndex: rf.lastIncludedIndex,
	}
	rf.applyChan <- applyMsg
}

// 投票rpc Handle
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

	if isLast {
		if args.Term > rf.currentTerm { //如果对方任期比自己大，自己变为follower,不投票
			rf.convert2Follower(args.Term)
			rf.persist()
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
	rf.persist()
}

// 作为candidate发送投票
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

	return
}

// 心跳rpc Handle
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
		rf.persist()
	}

	rf.timer.reset() //在convert2Follower有reset，但不一定保证能调用convert2Follower，所以需要手动reset

	if args.PrevLogIndex >= 0 && // leader要有日志
		(len(rf.log)-1 < args.PrevLogIndex || // 1. preLogIndex大于当前日志最大下标，说明缺失日志，拒绝附加
			rf.log[args.PrevLogIndex].Term != args.PrevLogTerm) { // 2. 在相同index下日志term不同

		reply.CommitIndex = len(rf.log) - 1        //1.通知leader需要从这个index开始同步
		if reply.CommitIndex > args.PrevLogIndex { //2.相同index下的term不同，需要缩小一下范围
			reply.CommitIndex = args.PrevLogIndex
		}
		if reply.CommitIndex < 0 {
			reply.Success = false // 返回false,此节点日志没有跟上leader，或者有多余日志，或者日志有冲突
			return
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
		rf.persist()
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

// 作为leader发送快照
func (rf *Raft) sendSnapShot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapShot", args, reply)
	return ok
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
		rf.persist()
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
			go rf.applyLogs() // 提交日志
		}

	} else { //对方不同意提交，需要更改nextIndex
		rf.nextIndex[server] = reply.CommitIndex + 1
		if rf.nextIndex[server] > len(rf.log) {
			rf.nextIndex[server] = len(rf.log)
		}
	}
}

// 外部向leader增加一条日志
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return index, term, false
	}
	e := logEntry{command, rf.currentTerm}
	rf.log = append(rf.log, e)
	index = len(rf.log)
	term = rf.currentTerm
	rf.persist()
	return index, term, true
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 定时器
func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.timer.timer.C:
			rf.mu.Lock()
			switch rf.state {
			case Follower: // follower变candidate
				rf.state = Candidate
				fallthrough
			case Candidate: // 成为候选人，开始拉票
				rf.currentTerm++
				rf.voteCount = 1
				rf.timer.reset()
				rf.votedFor = rf.me
				rf.persist()
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
				rf.timer.resetHeartBeat()
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}

					if rf.nextIndex[i]-1 < rf.lastIncludedIndex { //有快照，加速更新
						// go rf.leaderSendSnapShot(i)
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

// 初始化raft节点
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}            // 创建一个新的Raft实例
	rf.peers = peers         // 初始化peers，存储所有同伴服务器的网络端点
	rf.persister = persister // 持久化对象，用于读写Raft的持久状态
	rf.me = me               // 设置当前节点在peers数组中的索引

	// Your initialization code here (3A, 3B, 3C).
	rf.applyChan = applyCh
	rf.currentTerm = 0           //当前的任期
	rf.votedFor = -1             //没有投票给别人
	rf.log = make([]logEntry, 0) //日志

	rf.commitIndex = -1       // 初始化时没有任何提交的日志条目
	rf.lastApplied = -1       // 初始化时没有应用到状态机的日志条目
	rf.lastIncludedTerm = -1  //初始化快照Term
	rf.lastIncludedIndex = -1 //初始化快照Index

	rf.nextIndex = make([]int, len(peers))  // 对于每个同伴，记录下一个要发送的日志条目索引，最开始为0
	rf.matchIndex = make([]int, len(peers)) // 对于每个同伴，记录已经复制给他们的日志的最高索引，最开始为-1
	for i := range rf.matchIndex {
		rf.matchIndex[i] = -1
	}

	rf.state = Follower                                                                           //跟随者
	rf.voteCount = 0                                                                              //投票数
	rf.timer = Timer{timer: time.NewTicker(time.Duration(150+rand.Intn(200)) * time.Millisecond)} // 设置定时器

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState()) // 从持久化存储中读取之前的状态

	// 同步快照信息
	if rf.lastIncludedIndex >= 0 {
		rf.lastApplied = rf.lastIncludedIndex
	}
	// start ticker goroutine to start elections
	go rf.ticker() // 启动一个后台goroutine，负责定期触发选举和心跳等

	return rf
}

func (rf *Raft) convert2Follower(term int) {
	//fmt.Println("变更为follower svr:", rf.me, "任期变为:", term)
	rf.currentTerm = term
	rf.state = Follower
	rf.voteCount = 0
	rf.votedFor = -1
	rf.timer.reset()
}

// 将日志写入管道
func (rf *Raft) applyLogs() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex > len(rf.log)-1 {
		fmt.Println("不合法")
	}

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		rf.applyChan <- ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command,
			CommandIndex: i + 1,
		}
	}
	rf.lastApplied = rf.commitIndex
}
