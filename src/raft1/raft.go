package raft

import (
	//	"bytes"
	"log/slog"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
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
	votes    int
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

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == "leader"
	return term, isleader
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

    // 1️⃣ 如果候选人 term 比自己小，直接拒绝
    if args.Term < rf.currentTerm {
        return
    }

    // 2️⃣ 如果 term 更大，更新自己
    if args.Term > rf.currentTerm {
        rf.currentTerm = args.Term
        rf.votedFor = -1
        rf.state = "follower"
    }

    // 3️⃣ 检查候选人日志是否至少一样新
    myLastTerm := rf.log[len(rf.log)-1].Term
    myLastIndex := len(rf.log) - 1
    if args.LastLogTerm < myLastTerm ||
        (args.LastLogTerm == myLastTerm && args.LastLogIndex < myLastIndex) {
        return
    }

    // 4️⃣ 如果还没投票或之前投给同一个人
    if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
        rf.votedFor = args.CandidateId
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
    rf.currentTerm += 1
    rf.state = "candidate"
    rf.votedFor = rf.me
    rf.resetElectionTimer()

    lastIndex := len(rf.log) - 1
    lastTerm := rf.log[lastIndex].Term

    term := rf.currentTerm
    rf.mu.Unlock()

    args := RequestVoteArgs{
        Term:         term,
        CandidateId:  rf.me,
        LastLogIndex: lastIndex,
        LastLogTerm:  lastTerm,
    }

    votes := 1
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
                    rf.state = "follower"
                    rf.votedFor = -1
                    return
                }

                if reply.VoteGranted {
                    votes++
                    if votes > len(rf.peers)/2 && rf.state == "candidate" {
                        rf.state = "leader"
                        for m := range rf.peers {
							rf.nextIndex[m] = len(rf.log)
							rf.matchIndex[m] = rf.lastApplied
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
	Entry        []Log
	PrevLogIndex int
	PrevLogTerm  int
	CommitIndex  int
}
type AppendEntriesReply struct {
	Term        int
	CommitIndex int
	Success     bool
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
				prevTerm := rf.log[prevIndex].Term
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
					Entry:        entries,
					PrevLogTerm:  prevTerm,
					PrevLogIndex: prevIndex,
					CommitIndex:  commandIndex,
				}
				if args.Entry != nil {
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
					rf.state = "follower"
					rf.votedFor = -1
					rf.mu.Unlock()
					return
				}
				if reply.Success {
					rf.matchIndex[server] = args.PrevLogIndex + len(args.Entry)
					rf.nextIndex[server] = rf.matchIndex[server] + 1
				} else {
					if rf.nextIndex[server] > 1 {
						rf.nextIndex[server]--
					} else {
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
	reply.CommitIndex = rf.commitIndex
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = "follower"
	}

	rf.resetElectionTimer()

	if args.PrevLogIndex >= len(rf.log) ||
		rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		if args.PrevLogIndex < len(rf.log) {
			rf.log = rf.log[:args.PrevLogIndex]
		}
		return
	}
	// if args.Entry != nil {
	// 	slog.Info("leader log", "id", rf.me, "leadID", rf.me, "my term", rf.currentTerm, " log", rf.log, "---need join", args.Entry)
	// }

	if len(rf.log) > 0 {
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entry...)
	}

	if args.CommitIndex > rf.commitIndex {
		lastNewEntry := len(rf.log) - 1
		if args.CommitIndex < lastNewEntry {
			rf.commitIndex = args.CommitIndex
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
	rf.currentTerm = 0
	rf.votedFor = -1
	ms := time.Duration(150+(rand.Int63()%150)) * time.Millisecond
	rf.electionTimeout = ms
	// Your initialization code here (3A, 3B, 3C).
	//3B
	rf.applyCh = applyCh
	rf.log = make([]Log, 1)
	rf.log[0] = Log{Term: 0, Command: nil}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := range rf.peers {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = len(rf.log)
	}
	rf.lastHeartbeat = time.Now()
	// 从崩溃前保存的状态初始化
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
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
