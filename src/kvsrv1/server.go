package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}


type KVServer struct {
	mu sync.Mutex
	Store map[string]string
	Versions map[string]uint64
	// Your definitions here.
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		Store: make(map[string]string),
		Versions: make(map[string]uint64),
	}
	// Your code here.
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	value,ok := kv.Store[key]
	if ok{
		reply.Value = value
		reply.Version = rpc.Tversion(kv.Versions[key])
		reply.Err = rpc.OK
	}else{
		reply.Value = ""
		reply.Version = 0
		reply.Err = rpc.ErrNoKey
		return
	}
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
// 如果args.Version与服务器上键的版本匹配，则更新键的值。如果版本不匹配，则返回ErrVersion错误。如果键不存在，当args.Version为0时，Put将安装该值，否则返回ErrNoKey错误。
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	key := args.Key
	value := args.Value
	version := args.Version

	_, ok := kv.Store[key]
	currentVersion := kv.Versions[key]
	if !ok {
		if version == 0 {
			kv.Store[key] = value
			kv.Versions[key] = 1
			reply.Err = rpc.OK
		} else {
			reply.Err = rpc.ErrNoKey
		}
		return
	}
	if rpc.Tversion(currentVersion) != version {
		reply.Err = rpc.ErrVersion
		return
	}
	kv.Store[key] = value
	kv.Versions[key] = currentVersion + 1
	reply.Err = rpc.OK
}

// You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}


// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
