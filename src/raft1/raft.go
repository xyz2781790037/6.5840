package raft

import (
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
	"bytes"
	"log/slog"
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
func (rf *Raft) persist() {
	// Your code here (3C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, nil)
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
	if d.Decode(&term) != nil ||
		d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
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
}

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all Debug up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
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
	}

	myLastIndex := len(rf.log) - 1
	myLastTerm := rf.log[myLastIndex].Term
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
	index := len(rf.log) - 1
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

	lastIndex := len(rf.log) - 1
	lastTerm := rf.log[lastIndex].Term

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
						rf.state = "leader"
						for m := range rf.peers {
							rf.nextIndex[m] = len(rf.log)
							rf.matchIndex[m] = 0
						}
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
				term := rf.currentTerm
				leadId := rf.me
				commandIndex := rf.commitIndex
				prevIndex := rf.nextIndex[server] - 1
				if prevIndex < 0 {
					prevIndex = 0
				}
				var prevTerm int

				if prevIndex >= 0 && prevIndex < len(rf.log) {
					prevTerm = rf.log[prevIndex].Term
				} else {
					return
				}

				var entries []Log
				if prevIndex+1 < len(rf.log) {
					entries = make([]Log, len(rf.log[prevIndex+1:]))
					copy(entries, rf.log[prevIndex+1:])
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
				if args.Entries != nil {
					slog.Debug("sendAE", "args", args)
				}
				reply := AppendEntriesReply{}
				ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
				rf.mu.Lock()
				if !ok {
					rf.mu.Unlock()
					return
				}
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.votedFor = -1
					rf.persist()
					rf.state = "follower"
					rf.mu.Unlock()
					return
				}
				if reply.Success {
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
				} else {
					if reply.XTerm == -1 {
						rf.nextIndex[server] = reply.XLen
					} else {
						lastIndexForTerm := -1
						for i := args.PrevLogIndex; i >= 0; i-- {
							if rf.log[i].Term == reply.XTerm {
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
					
					if rf.nextIndex[server] < 1 {
						rf.nextIndex[server] = 1
					}
				}
				rf.mu.Unlock()
			}(server)

		}
		rf.mu.Lock()
		for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
			if rf.log[N].Term != rf.currentTerm {
				continue
			}
			count := 1
			for j := range rf.peers {
				if j == rf.me {
					continue
				}
				if rf.matchIndex[j] >= N && rf.log[N].Term == rf.currentTerm {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				break
			}
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
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
	}

	rf.resetElectionTimer()

	if args.PrevLogIndex >= len(rf.log) {
		reply.XTerm = -1
		reply.XLen = len(rf.log)
		return
	}
	if args.PrevLogIndex >= 0 &&
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		for i := args.PrevLogIndex; i >= 0; i-- {
			if rf.log[i].Term != reply.XTerm {
				reply.XIndex = i + 1
				break
			}
			if i == 0 {
				reply.XIndex = 0
				break
			}
		}
		return
	}
	i := 0
	for ; i < len(args.Entries); i++ {
		idx := args.PrevLogIndex + 1 + i
		if idx < len(rf.log) {
			if rf.log[idx].Term != args.Entries[i].Term {
				rf.log = rf.log[:idx]
				break
			}
		}else{
			break
		}
	}
	if i < len(args.Entries) {
		rf.log = append(rf.log, args.Entries[i:]...)
		rf.persist()
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
	// 从崩溃前保存的状态初始化
	rf.readPersist(persister.ReadRaftState())
	rf.persist()
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range rf.peers {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.log)
	}
	go rf.ticker()
	go rf.applier()
	return rf
}
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex <= rf.lastApplied {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
			continue
		}
		start := rf.lastApplied + 1
		end := rf.commitIndex + 1
		entries := make([]Log, end-start)
		copy(entries, rf.log[start:end])
		rf.mu.Unlock()
		for i, entry := range entries {
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: start + i,
			}
			rf.applyCh <- msg
			rf.mu.Lock()
			rf.lastApplied = start + i
			rf.mu.Unlock()
		}
	}
}
