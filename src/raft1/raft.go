package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	"6.5840/tester1"
)

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
	applyCh   chan raftapi.ApplyMsg
	
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
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	MeId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	VotedFor int
	Id       int
	Term     int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	if args.Term >= rf.currentTerm {
		rf.state = "follower"
		if args.Term == rf.currentTerm && rf.votedFor == -1 {
			reply.VotedFor = args.MeId
		} else {
			reply.VotedFor = rf.votedFor
		}

	} else {
		reply.VotedFor = rf.votedFor
	}
	reply.Term = rf.currentTerm
	rf.currentTerm = args.Term
	reply.Id = rf.me
	rf.resetElectionTimer()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 使用 Raft 的服务（例如一个键/值服务器）想要启动对下一个要附加到 Raft 日志的命令达成一致。如果此服务器不是领导者，则返回 false。否则启动一致性协议并立即返回。不能保证此命令最终会被提交到 Raft 日志，因为领导者可能会失败或在选举中失利。即使 Raft 实例已被终止，此函数也应优雅地返回。第一个返回值是命令最终被提交时在日志中出现的索引。第二个返回值是当前任期。第三个返回值为 true，如果此服务器认为自己是领导者。
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (3B).

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
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

		// Your code here (3A)
		// 检查是否应该启动领导者选举
		rf.mu.Lock()
		if time.Since(rf.lastHeartbeat) > rf.electionTimeout {
			fmt.Println("id is",rf.me)
			rf.startElection()
		}
		
		rf.mu.Unlock()

		ms := 150 + (rand.Int63() % 150)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
func (rf *Raft) resetElectionTimer() {
	rf.electionTimeout = time.Duration(150+rand.Int63()%150) * time.Millisecond
	rf.lastHeartbeat = time.Now()
}
func (rf *Raft) startElection() {
	rf.currentTerm++
	rf.state = "candidate"
	rf.votes = 1
	peerCount := len(rf.peers)
	args := RequestVoteArgs{
		Term: rf.currentTerm,
		MeId: rf.me,
	}
	rf.resetElectionTimer()
	for i := 0; i < peerCount; i++ {
		reply := RequestVoteReply{}
		if i == rf.me {
			continue
		}
		go func(server int) {
			ok := rf.sendRequestVote(server, &args, &reply)
			if ok {
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.state = "follower"
					rf.lastHeartbeat = time.Now()
					rf.votedFor = -1
					return
				}
				rf.mu.Lock()
				rf.votes++
				if rf.votes > peerCount/2 && rf.state == "candidate" {
					fmt.Println("leader is",rf.me)
					rf.state = "leader"
					rf.lastHeartbeat = time.Now()
					go rf.sendAppendEntries()
				}
				rf.mu.Unlock()
			}
		}(i)
	}
}
type AppendEntriesArgs struct {
	Term int
	LeaderId     int
}
type AppendEntriesReply struct {
	Term int
	Success bool
}
func (rf *Raft)sendAppendEntries(){
	args := AppendEntriesArgs{}

	for i:= 0;i < len(rf.peers);i++{
		reply := AppendEntriesReply{}
		ok := rf.peers[i].Call("Raft.AppendEntries", &args, &reply)
		if ok{
			if reply.Term > rf.currentTerm && !reply.Success {
				fmt.Println("heartBeat failed")
			}
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
		rf.state = "follower"
	}
	rf.resetElectionTimer()
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
	fmt.Println("election time is",ms)
	rf.electionTimeout = ms
	// Your initialization code here (3A, 3B, 3C).
	//3B
	rf.applyCh = applyCh
	// persister.Save()
	// state := make([]byte,1)
	// state = append(state, rf)
	// 从崩溃前保存的状态初始化
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
