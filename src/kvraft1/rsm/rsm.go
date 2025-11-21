package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

var useRaftStateMachine bool // to plug in another raft besided raft1

type Op struct {
	// Your definitions here.
	OpType   string
	Key      string
	Value    string
	Version   rpc.Tversion
}

// A server (i.e., ../server.go) that wants to replicate itself calls
// MakeRSM and must implement the StateMachine interface.  This
// interface allows the rsm package to interact with the server for
// server-specific operations: the server must implement DoOp to
// execute an operation (e.g., a Get or Put request), and
// Snapshot/Restore to snapshot and restore the server's state.
type StateMachine interface {
	DoOp(any) any
	Snapshot() []byte
	Restore([]byte)
}

type RSM struct {
	mu           sync.Mutex
	me           int
	rf           raftapi.Raft
	applyCh      chan raftapi.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	sm           StateMachine
	// Your definitions here.
	persister   *tester.Persister
	waitChs     map[int]chan any
	lastApplied int
	lastResult  map[int]any // 存储每个索引的应用结果
	lastOp      map[int]any // 存储每个索引的操作
}

// any long-running work./ servers[] 包含一组服务器的端口，这些服务器将通过 Raft 协作
// 以形成容错的键/值服务。

// me 是当前服务器在 servers[] 中的索引。

// 键/值服务器应该通过底层 Raft 实现来存储快照，
// Raft 应调用 persister.SaveStateAndSnapshot() 来
// 原子地保存 Raft 状态及快照。
// 当 Raft 保存的状态超过 maxraftstate 字节时，RSM 应该生成快照，
// 以允许 Raft 对其日志进行垃圾回收。如果 maxraftstate 为 -1，
// 则不需要生成快照。

// MakeRSM() 必须快速返回，因此它应该为任何长期运行的任务启动 goroutine。
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		persister:    persister,
		waitChs:      make(map[int]chan any),
		lastApplied:  0,
		lastResult:   make(map[int]any),
		lastOp:       make(map[int]any),
	}
	if !useRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.applier()
	return rsm
}

func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

func (rsm *RSM) Submit(req any) (rpc.Err, any) {
	index, _, isLeader := rsm.rf.Start(req)
	if !isLeader {
		return rpc.ErrWrongLeader, nil
	}

	ch := make(chan any, 1)
	rsm.mu.Lock()
	// 检查是否已经应用了这个操作
	if result, exists := rsm.lastResult[index]; exists {
		if storedOp, opExists := rsm.lastOp[index]; opExists {
			if rsm.isSameOperation(storedOp, req) {
				rsm.mu.Unlock()
				return rpc.OK, result
			}
		}
	}
	rsm.waitChs[index] = ch
	rsm.mu.Unlock()

	timeout := time.After(2000 * time.Millisecond)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case result := <-ch:
			if _, isLeader := rsm.rf.GetState(); !isLeader {
				rsm.mu.Lock()
				delete(rsm.waitChs, index)
				rsm.mu.Unlock()
				return rpc.ErrWrongLeader, nil
			}
			return rpc.OK, result

		case <-timeout:
			rsm.mu.Lock()
			delete(rsm.waitChs, index)
			rsm.mu.Unlock()
			return rpc.ErrWrongLeader, nil
		}
	}
}
func (rsm *RSM) applier() {
	for msg := range rsm.applyCh {
		if msg.CommandValid {
			rsm.mu.Lock()

			if msg.CommandIndex <= rsm.lastApplied {
				rsm.mu.Unlock()
				continue
			}
			result := rsm.sm.DoOp(msg.Command)
			rsm.lastResult[msg.CommandIndex] = result
			rsm.lastOp[msg.CommandIndex] = msg.Command

			if ch, ok := rsm.waitChs[msg.CommandIndex]; ok {
				select {
				case ch <- result:
				default:
				}
				delete(rsm.waitChs, msg.CommandIndex)
			}
			rsm.lastApplied = msg.CommandIndex
			if rsm.maxraftstate != -1 && rsm.persister.RaftStateSize() > rsm.maxraftstate {
				snapshot := rsm.sm.Snapshot()
				rsm.rf.Snapshot(msg.CommandIndex, snapshot)
			}
			rsm.mu.Unlock()
		}
		if msg.SnapshotValid {
			rsm.mu.Lock()
			rsm.sm.Restore(msg.Snapshot)
			rsm.lastApplied = msg.SnapshotIndex
			rsm.lastResult = make(map[int]any)
			rsm.lastOp = make(map[int]any)
			rsm.mu.Unlock()
		}
	}
}
func (rsm *RSM) isSameOperation(op1, op2 any) bool {
	switch o1 := op1.(type) {
	case Op:
		if o2, ok := op2.(Op); ok {
			return o1.OpType == o2.OpType &&
				o1.Key == o2.Key &&
				o1.Value == o2.Value &&
				o1.Version == o2.Version
		}
	}
	return false
}