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
- 如果日志以相同的任期结尾，则较长的日志更新程度更高





# 关键变量

## CommitIndex

### 含义

该server已经提交了的日志索引

### 特性

1. 每一个server的CommitIndex**只增不减**

2. 集群中，follower的CommitIndex可能会大于leader的

   > 原因：老leader根据MatchIndex发现CommitIndex可以增加，但是发送心跳同步follower的时候突然掉线，那么follower会当选新leader，这个时候，老leader恢复，发送心跳发现自己的Term更小，变成follower，此时follower的CommitIndex>leader的CommitIndex

3. 只有当绝大多数server认为可以提交的时候，leader才会提交，并且增加CommitIndex的时机在于**下一次**leader发送rpc心跳







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

## 投票

## 心跳

## 快照

# 持久化

每个server在以下三个变量变化的时候，进行持久化操作

```go
currentTerm
votedFor
log[]
```





# 快照

每个节点都会**存取自身的快照，快照的信息就相当于commit过后的日志。**