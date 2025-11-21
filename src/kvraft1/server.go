package kvraft

import (
	"sync/atomic"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/tester1"
	"sync"
	"reflect"
)

type KVServer struct {
	me   int
	dead int32 // set by Kill()
	rsm  *rsm.RSM
	// Your definitions here.
	mu      sync.Mutex
	data    map[string]string
	version map[string]rpc.Tversion
}

// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// 核弹级防 panic 神器（治愈所有 reflect 病）
	rv := reflect.ValueOf(req)
	if rv.Kind() != reflect.Pointer {
		ptr := reflect.New(rv.Type())
		ptr.Elem().Set(rv)
		req = ptr.Interface()
	}

	switch args := req.(type) {
	case *rpc.GetArgs:
		val, ok := kv.data[args.Key]
		if !ok {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		ver := kv.version[args.Key]
		return rpc.GetReply{Value: val, Version: ver, Err: rpc.OK}
	case *rpc.PutArgs:
		ver, ok := kv.version[args.Key]
		if !ok {
			ver = 0
		}
		if args.Version != ver {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		kv.data[args.Key] = args.Value
		kv.version[args.Key] = ver + 1
		return rpc.PutReply{Err: rpc.OK}
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// Your code here
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// Your code here
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, rep := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	if rep == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	r := rep.(rpc.GetReply)
	reply.Value = r.Value
	reply.Version = r.Version
	reply.Err = r.Err
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, rep := kv.rsm.Submit(args)
	if err != rpc.OK {
		reply.Err = err
		return
	}
	if rep == nil {
		reply.Err = rpc.ErrWrongLeader
		return
	}
	r := rep.(rpc.PutReply)
	reply.Err = r.Err
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// StartKVServer() and MakeRSM() must return quickly, so they should
// start goroutines for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []tester.IService {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}


	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	kv.data = make(map[string]string)
	kv.version = make(map[string]rpc.Tversion)
	// You may need initialization code here.
	return []tester.IService{kv, kv.rsm.Raft()}
}
