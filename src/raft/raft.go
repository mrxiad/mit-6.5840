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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

const (
	Follower  = iota //0
	Candidate        //1
	Leader           //2
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
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

const HeartBeatTimeout = 50 * time.Millisecond //20ms

func (t *Timer) resetHeartBeat() { //心跳时间
	t.timer.Reset(HeartBeatTimeout)
}

// A Go object implementing a single Raft peer.
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

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var isleader = false
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		isleader = true
	}
	//fmt.Println("svrId state[id],[term],[state]:", rf.me, rf.currentTerm, rf.state)
	return rf.currentTerm, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // 候选人的任期
	CandidateId  int // 候选人ID
	LastLogIndex int // 候选人最后的日志索引
	LastLogTerm  int // 候选人最后的日志任期
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // 投票人的当前任期
	VoteGranted bool // true表示该节点把票投给了候选人
}

// 心跳
type AppendEntriesArgs struct {
	Term         int // leader's term
	LeaderId     int
	PrevLogIndex int        // leader认为follower已经复制到了此位置
	PrevLogTerm  int        // 位于 PrevLogIndex 的日志条目的任期号
	Entries      []logEntry //需要被复制的日志条目的集合。在心跳消息中，这个数组可能为空，表示没有新的日志条目需要被复制，仅仅是为了维护心跳和领导地位。

	LeaderCommit int //leader的commitIndex
}

type AppendEntriesReply struct {
	Term        int  // leader's term
	Success     bool // 如果为true，则说明leader可能更新commitIndex
	CommitIndex int  // follower通知leader自己的CommitIndex信息，更新leader的nextIndex[i]
}

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

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
/*
	如果该server是leader，则增加一条日志，否则退出
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return index, term, false
	}

	e := logEntry{command, rf.currentTerm} //日志
	rf.log = append(rf.log, e)
	index = len(rf.log)
	term = rf.currentTerm

	return index, term, true
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

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

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
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

	rf.commitIndex = -1 // 初始化时没有任何提交的日志条目
	rf.lastApplied = -1 // 初始化时没有应用到状态机的日志条目

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

	// start ticker goroutine to start elections
	go rf.ticker() // 启动一个后台goroutine，负责定期触发选举和心跳等

	return rf
}

// 当前节点变为follower，任期改成term
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
