package raftapi

// The Raft interface
type Raft interface {
	// Start agreement on a new log entry, and return the log index
	// for that entry, the term, and whether the peer is the leader.
	Start(command interface{}) (int, int, bool)

	// Ask a Raft for its current term, and whether it thinks it is
	// leader
	GetState() (int, bool)

	// For Snaphots (3D)
	Snapshot(index int, snapshot []byte)
	PersistBytes() int

	// For the tester to indicate to your code that is should cleanup
	// any long-running go routines.
	Kill()
}

// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the server (or
// tester), via the applyCh passed to Make(). Set CommandValid to true
// to indicate that the ApplyMsg contains a newly committed log entry.
//
// In Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
// 当每个 Raft 节点意识到连续的日志条目已提交时，节点应通过传递给 Make() 的 applyCh 向服务器（或测试器）发送 ApplyMsg。将 CommandValid 设置为 true，以表明 ApplyMsg 包含新提交的日志条目。在实验 3 中，你可能需要在 applyCh 上发送其他类型的消息（例如，快照）；此时你可以向 ApplyMsg 添加字段，但对于这些其他用途，应将 CommandValid 设置为 false。
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}