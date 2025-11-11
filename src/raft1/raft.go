package raft

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"bytes"
	"log/slog"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

type Log struct {
	Term    int         // 该日志条目属于哪个任期（term）
	Command interface{} // 客户端请求的命令（比如 “put x=5”）
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *tester.Persister   // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	currentTerm int

	LeaderId int
	votedFor int
	state    string

	lastHeartbeat   time.Time     // 最后心跳检测
	electionTimeout time.Duration //正常超时时间
	// Your data here (3A, 3B, 3C).
	applyCh     chan raftapi.ApplyMsg
	log         []Log
	commitIndex int   //被提交的最大字段
	nextIndex   []int //下一个日志
	matchIndex  []int //follower已经执行的
	lastApplied int   //被应用的字段
	// state a Raft server must maintain.
	// 3D
	lastIncludedIndex int
	lastIncludedTerm  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == "leader"
}

// 将 Raft 的持久状态保存到稳定存储中， 崩溃后可以恢复。 参考论文的图 2，了解哪些内容应保持持久化。 在实现快照之前，应将 nil 作为 persister.Save() 的第二个参数。 在实现快照之后，传递当前的快照
// (or nil if there's not yet a snapshot).
func (rf *Raft) encodeRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	return w.Bytes()
}
func (rf *Raft) persist() {
	// Your code here (3C).
	rf.persister.Save(rf.encodeRaftState(), rf.persister.ReadSnapshot())
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var votedFor int
	var logs []Log
	var lastIncludeIndex int
	var lastIncludeTerm int
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logs) != nil {
		slog.Warn("readPersist decode failed")
		return
	} else {
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.log = logs
		if len(rf.log) == 0 {
			rf.log = append(rf.log, Log{Term: 0})
		}
	}
	if d.Decode(&lastIncludeIndex) == nil && d.Decode(&lastIncludeTerm) == nil {
		rf.lastIncludedIndex = lastIncludeIndex
		rf.lastIncludedTerm = lastIncludeTerm
	}
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

type InstallSnapshotArgs struct {
	Term              int
	LastIncludedIndex int
	LastIncludedTerm  int
	Offset            int
	Data              []byte
	Done              bool
}
type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) sendInstallSnapshot(server int) {
	rf.mu.Lock()
	if rf.killed() {
		rf.mu.Unlock()
		return
	}
	if rf.state != "leader" {
		rf.mu.Unlock()
		return
	}
	snapshot := rf.persister.ReadSnapshot()
	if len(snapshot) == 0 {
		rf.mu.Unlock()
		return
	}
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Offset:            0,
		Data:              snapshot,
		Done:              true,
	}
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = "follower"
		slog.Info("Dead in \033[36msendInstallSnapshot\033[0m", "old status", rf.state, "Id", rf.me, "inTerm", reply.Term, "outTerm", rf.currentTerm)
		rf.votedFor = -1
		rf.persist()
		rf.mu.Unlock()
		return
	}

	// rf.nextIndex[server] = args.LastIncludedIndex + 1
	// rf.matchIndex[server] = args.LastIncludedIndex
	// rf.updateCommitIndex()
	if args.LastIncludedIndex > rf.matchIndex[server] {
		old := rf.matchIndex[server]
		rf.matchIndex[server] = args.LastIncludedIndex
		rf.nextIndex[server] = args.LastIncludedIndex + 1
		slog.Info("\033[35msendInstallSnapshot\033[0m: update matchIndex","Status",rf.state ,"server", server, "old", old, "new", rf.matchIndex[server])
		if rf.state == "leader" {
			rf.mu.Unlock()
			rf.updateCommitIndex()
			slog.Info("\033[1;33mfrom sendInstallSnapshot to updata CommitIndex\033[0m")
		}else{
			rf.mu.Unlock()
		}
	}else{
		rf.mu.Unlock()
	}

}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = "follower"
		slog.Info("Dead \033[36mInstallSnapshot\033[0m", "old status", rf.state, "Id", rf.me)
		rf.votedFor = -1
		rf.persist()
	}
	rf.resetElectionTimer()

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	oldLastIncludedIndex := rf.lastIncludedIndex
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	newLog := []Log{{Term: args.LastIncludedTerm}}
	startIdx := args.LastIncludedIndex + 1
	sliceStart := startIdx - oldLastIncludedIndex

	if sliceStart >= 0 && sliceStart < len(rf.log) {
		newLog = append(newLog, rf.log[sliceStart:]...)
	}
	rf.log = newLog

	rf.commitIndex = args.LastIncludedIndex
	rf.lastApplied = args.LastIncludedIndex
	slog.Info("follower recover \033[1;35mInstallSnapshot\033[0m", "commitIndex=lastApplied", rf.commitIndex, "log", rf.log)
	rf.persister.Save(rf.encodeRaftState(), args.Data)

	msg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.mu.Unlock()

	go func() { rf.applyCh <- msg }()
}

// the service says it has created a snapshot that has all Debug up to and including index. this means the service no longer needs the log through (and including) that index. Raft should now trim its log as much as possible.
// 服务表示它已经创建了一个包含所有调试信息（直到索引）的快照。这意味着服务不再需要该索引（包括该索引）的日志。Raft 现在应该尽可能多地修剪其日志。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex || index > rf.getLastLogIndex() {
		return
	}

	sliceIndex := rf.logIndexToSliceIndex(index)
	if sliceIndex < 0 || sliceIndex >= len(rf.log) {
		return
	}
	slog.Info("\033[1;32mold status\033[0m", "log", rf.log[sliceIndex:])
	rf.lastIncludedTerm = rf.log[sliceIndex].Term
	rf.lastIncludedIndex = index

	newLog := make([]Log, 0)
	newLog = append(newLog, Log{Term: rf.lastIncludedTerm})
	if sliceIndex+1 < len(rf.log) {
		newLog = append(newLog, rf.log[sliceIndex+1:]...)
	}
	rf.log = newLog
	slog.Info("\033[1;32mnew status\033[0m", "new log", rf.log)
	// 更新 commitIndex 和 lastApplied
	if rf.commitIndex < index {
		rf.commitIndex = index
	}
	if rf.lastApplied < index {
		rf.lastApplied = index
	}

	rf.persister.Save(rf.encodeRaftState(), snapshot)
}
func (rf *Raft) getLastLogIndex() int {
	return rf.lastIncludedIndex + len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) > 0 {
		return rf.log[len(rf.log)-1].Term
	}
	return rf.lastIncludedTerm
}

func (rf *Raft) logIndexToSliceIndex(logIndex int) int {
	return logIndex - rf.lastIncludedIndex
}
func (rf *Raft) getLogTerm(logIndex int) int {
	if logIndex < rf.lastIncludedIndex {
		return -1
	}
	if logIndex == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	sliceIndex := rf.logIndexToSliceIndex(logIndex)
	if sliceIndex >= 0 && sliceIndex < len(rf.log) {
		return rf.log[sliceIndex].Term
	}
	return -1
}

type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		rf.state = "follower"
		slog.Info("Dead in \033[36mRequestVote\033[0m", "old status", rf.state, "Id", rf.me)
	}

	myLastIndex := rf.getLastLogIndex()
	myLastTerm := rf.getLastLogTerm()
	if args.LastLogTerm < myLastTerm ||
		(args.LastLogTerm == myLastTerm && args.LastLogIndex < myLastIndex) {
		return
	}

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		rf.persist()
		reply.VoteGranted = true
		rf.resetElectionTimer()
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 使用 Raft 的服务（例如一个键/值服务器）想要启动对下一个要附加到 Raft 日志的命令达成一致。如果此服务器不是领导者，则返回 false。否则启动一致性协议并立即返回。不能保证此命令最终会被提交到 Raft 日志，因为领导者可能会失败或在选举中失利。即使 Raft 实例已被终止，此函数也应优雅地返回。第一个返回值是命令最终被提交时在日志中出现的索引。第二个返回值是当前任期。第三个返回值为 true，如果此服务器认为自己是领导者。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != "leader" {
		return -1, rf.currentTerm, false
	}
	entry := Log{
		Term:    rf.currentTerm,
		Command: command,
	}
	// slog.Debug("new log","Log",entry)
	rf.log = append(rf.log, entry)
	rf.persist()
	index := rf.getLastLogIndex()
	rf.matchIndex[rf.me] = index
	rf.nextIndex[rf.me] = index + 1
	return index, rf.currentTerm, true
}

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

		// Your code here (3A)
		// 检查是否应该启动领导者选举
		rf.mu.Lock()
		State := rf.state
		lastTime := rf.lastHeartbeat
		timeout := rf.electionTimeout
		rf.mu.Unlock()
		if time.Since(lastTime) > timeout && State != "leader" {
			rf.startElection()
		}
		ms := 150 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
func (rf *Raft) resetElectionTimer() {
	rf.electionTimeout = time.Duration(150+rand.Int63()%150) * time.Millisecond
	rf.lastHeartbeat = time.Now()
}
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.currentTerm++
	rf.persist()
	rf.state = "candidate"
	rf.votedFor = rf.me
	rf.persist()
	rf.resetElectionTimer()

	lastIndex := rf.getLastLogIndex()
	lastTerm := rf.getLastLogTerm()

	term := rf.currentTerm
	me := rf.me
	rf.mu.Unlock()

	args := RequestVoteArgs{
		Term:         term,
		CandidateId:  me,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}

	var votes int32 = 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			var reply RequestVoteReply
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.persist()
					rf.state = "follower"
					slog.Info("Dead \033[36mstartElection\033[0m", "old status", rf.state, "Id", rf.me)
					rf.votedFor = -1
					rf.persist()
					return
				}
				if rf.state != "candidate" || rf.currentTerm != args.Term {
					return
				}
				if reply.VoteGranted {
					atomic.AddInt32(&votes, 1)
					if atomic.LoadInt32(&votes) > int32(len(rf.peers)/2) {
						slog.Info("New leader success", "leader", rf.me)
						rf.state = "leader"
						for m := range rf.peers {
							rf.nextIndex[m] = rf.getLastLogIndex() + 1
							rf.matchIndex[m] = rf.lastIncludedIndex
						}
						rf.matchIndex[rf.me] = rf.getLastLogIndex()
						rf.lastHeartbeat = time.Now()
						go rf.sendAppendEntries()
					}
				}
			}
		}(i)
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	XTerm   int // 冲突日志的 term
	XIndex  int // follower 中该 term 第一次出现的 index
	XLen    int // follower 日志总长度（有时用于额外优化）
}

func (rf *Raft) sendAppendEntries() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state != "leader" {
			rf.mu.Unlock()
			return
		}

		rf.mu.Unlock()

		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			server := i
			go func(server int) {
				rf.mu.Lock()
				if rf.state != "leader" {
					rf.mu.Unlock()
					return
				}
				if rf.nextIndex[server] < rf.lastIncludedIndex {
					rf.mu.Unlock()
					go rf.sendInstallSnapshot(server)
					return
				}
				term := rf.currentTerm
				leadId := rf.me
				commandIndex := rf.commitIndex
				prevIndex := rf.nextIndex[server] - 1
				if prevIndex < rf.lastIncludedIndex {
					rf.mu.Unlock()
					go rf.sendInstallSnapshot(server)
					return
				}
				if prevIndex > rf.getLastLogIndex() {
					// 如果 prevIndex 超出范围，重置为最后一个日志索引
					rf.nextIndex[server] = rf.getLastLogIndex() + 1
					rf.mu.Unlock()
					return
				}
				prevTerm := rf.getLogTerm(prevIndex)
				if prevTerm == -1 {
					rf.mu.Unlock()
					rf.sendInstallSnapshot(server)
					return
				}

				var entries []Log
				if rf.nextIndex[server] <= rf.getLastLogIndex() {
					sliceStart := rf.logIndexToSliceIndex(rf.nextIndex[server])
					if sliceStart >= 0 && sliceStart < len(rf.log) {
						entries = make([]Log, len(rf.log[sliceStart:]))
						copy(entries, rf.log[sliceStart:])
					}
				} else {
					entries = nil
				}

				rf.mu.Unlock()
				args := AppendEntriesArgs{
					Term:         term,
					LeaderId:     leadId,
					Entries:      entries,
					PrevLogTerm:  prevTerm,
					PrevLogIndex: prevIndex,
					LeaderCommit: commandIndex,
				}
				reply := AppendEntriesReply{}
				ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

				if !ok {
					return
				}
				rf.mu.Lock()
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.persist()
					rf.state = "follower"
					slog.Info("Dead \033[36msendAppendEntires\033[0m", "old status", rf.state, "Id", rf.me)
					rf.mu.Unlock()
					return
				}
				if rf.state != "leader" || rf.currentTerm != args.Term {
					rf.mu.Unlock()
					return
				}
				if reply.Success {
					old := rf.matchIndex[server]
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
					
					if rf.matchIndex[server] > old && rf.state == "leader"{
						rf.mu.Unlock()
						rf.updateCommitIndex()
						slog.Info("\033[1;33mfrom sendAppendEntries to updata CommitIndex\033[0m","ID",rf.me)
						rf.mu.Lock()
					}
					
					
				} else {
					if reply.XTerm == -1 {
						rf.nextIndex[server] = reply.XLen
					} else {
						lastIndexForTerm := -1
						for i := rf.getLastLogIndex(); i >= rf.lastIncludedIndex; i-- {
							if rf.getLogTerm(i) == reply.XTerm {
								lastIndexForTerm = i
								break
							}
						}
						if lastIndexForTerm >= 0 {
							rf.nextIndex[server] = lastIndexForTerm + 1
						} else {
							rf.nextIndex[server] = reply.XIndex
						}
					}

					if rf.nextIndex[server] < rf.lastIncludedIndex+1 {
						rf.nextIndex[server] = rf.lastIncludedIndex + 1
					}
					
				}
				rf.mu.Unlock()
			}(server)

		}

		time.Sleep(35 * time.Millisecond)
	}
}
func (rf *Raft) updateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for N := rf.getLastLogIndex(); N > rf.commitIndex; N-- {
		if rf.getLogTerm(N) != rf.currentTerm {
			continue
		}
		count := 1
		for j := range rf.peers {
			if j != rf.me && rf.matchIndex[j] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			old := rf.commitIndex
			rf.commitIndex = N
			slog.Info("\033[34mupdateCommtIndex\033[0m", "new", rf.commitIndex, "old", old)
			break
		}
	}
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
		rf.state = "follower"
		slog.Info("Dead \033[36mAppendEntries\033[0m", "old status", rf.state, "Id", rf.me)
	}

	rf.resetElectionTimer()

	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.XTerm = -1
		reply.XLen = rf.lastIncludedIndex + 1
		reply.Success = false
		return
	}
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.XTerm = -1
		reply.XLen = rf.getLastLogIndex() + 1
		reply.Success = false
		return
	}
	prevLogTerm := rf.getLogTerm(args.PrevLogIndex)
	if prevLogTerm != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = prevLogTerm
		conflictIndex := args.PrevLogIndex
		for conflictIndex > rf.lastIncludedIndex && rf.getLogTerm(conflictIndex-1) == reply.XTerm {
			conflictIndex--
		}
		reply.XIndex = conflictIndex
		return
	}
	for i, entry := range args.Entries {
		entryIndex := args.PrevLogIndex + 1 + i
		entrySliceIndex := rf.logIndexToSliceIndex(entryIndex)

		if entrySliceIndex < len(rf.log) {
			if rf.log[entrySliceIndex].Term != entry.Term {
				// 发现冲突，截断日志
				rf.log = rf.log[:entrySliceIndex]
				rf.log = append(rf.log, args.Entries[i:]...)
				rf.persist()
				break
			}
		} else {
			// 直接追加剩余条目
			rf.log = append(rf.log, args.Entries[i:]...)
			rf.persist()
			break
		}
	}
	if args.LeaderCommit > rf.commitIndex {
		lastNewEntry := args.PrevLogIndex + len(args.Entries)
		if args.LeaderCommit < lastNewEntry {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewEntry
		}
	}
	reply.Success = true
}
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.state = "follower"
	ms := time.Duration(150+(rand.Int63()%150)) * time.Millisecond
	rf.electionTimeout = ms
	// Your initialization code here (3A, 3B, 3C).
	//3B
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]Log, 1)
	rf.log[0] = Log{Term: 0, Command: nil}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastHeartbeat = time.Now()
	rf.lastIncludedTerm = 0
	rf.lastIncludedIndex = 0
	// 从崩溃前保存的状态初始化
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range rf.peers {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = rf.getLastLogIndex()
	}
	go rf.ticker()
	go rf.applier()
	return rf
}
func (rf *Raft) applier() {
    for !rf.killed() {
        rf.mu.Lock()

        // 应用所有已提交但未应用的日志条目
        var applyMsgs []raftapi.ApplyMsg
        for rf.lastApplied < rf.commitIndex {
            rf.lastApplied++
            applyIndex := rf.lastApplied

            // 检查是否在快照范围内
            if applyIndex <= rf.lastIncludedIndex {
                continue
            }

            sliceIndex := rf.logIndexToSliceIndex(applyIndex)
            if sliceIndex < 0 || sliceIndex >= len(rf.log) {
                // 如果索引超出范围，可能是状态不一致，重置 lastApplied
                rf.lastApplied = applyIndex - 1
                break
            }

            msg := raftapi.ApplyMsg{
                CommandValid: true,
                Command:      rf.log[sliceIndex].Command,
                CommandIndex: applyIndex,
            }
            applyMsgs = append(applyMsgs, msg)
        }
        rf.mu.Unlock()

        for _, msg := range applyMsgs {
            rf.applyCh <- msg
        }

        time.Sleep(10 * time.Millisecond)
    }
}
