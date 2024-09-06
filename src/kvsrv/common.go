package kvsrv

type MessageType int

const (
	Modify = iota
	Report
)

// Arguments for Put or Append operations
type PutAppendArgs struct {
	Key         string
	Value       string
	MessageType MessageType // Modify or Report
	MessageID   int64       // Unique ID for each message
}

type PutAppendReply struct {
	Value string
}

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value string
}
