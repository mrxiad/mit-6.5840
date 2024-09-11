package raft

import (
	"log"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

// 获取日志最后任期
func (rf *Raft) getLastTerm() int {
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1].Term
	}
	return rf.lastIncludedTerm
}

// 返还curIndex的日志的Term
func (rf *Raft) restoreLogTerm(curIndex int) int {
	if curIndex == rf.lastIncludedIndex { //由于curIndex-rf.lastIncludedIndex-1<0,所以直接返回即可
		return rf.lastIncludedTerm
	}
	return rf.log[curIndex-rf.lastIncludedIndex-1].Term
}

// 获取日志最后index,[0,index]合法
func (rf *Raft) getLastIndex() int {
	return len(rf.log) + rf.lastIncludedIndex
}

// 获取curIndex的日志
func (rf *Raft) restoreLog(curIndex int) logEntry {
	return rf.log[curIndex-rf.lastIncludedIndex-1]
}
