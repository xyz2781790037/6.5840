package kvraft

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
	"time"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	// You will have to modify this struct.
	lastLeader int
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt,
		servers:    servers,
		lastLeader: 0,
	}
	// You'll have to add code here.
	return ck
}

// 获取某个键的当前值和版本。如果该键不存在，它会返回 ErrNoKey。当遇到其他错误时，它会一直重试。

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := &rpc.GetArgs{Key: key}
	for {
		index := ck.lastLeader
		for range ck.servers {
			var reply rpc.GetReply
			ok := ck.clnt.Call(ck.servers[index], "KVServer.Get", args, &reply)
			if ok {
				if reply.Err == rpc.ErrWrongLeader {
					index = (index + 1) % len(ck.servers)
					continue
				}
				ck.lastLeader = index
				if reply.Err == rpc.OK {
					return reply.Value, reply.Version, rpc.OK
				}
				if reply.Err == rpc.ErrNoKey {
					return "", 0, rpc.ErrNoKey
				}
			}
			index = (index + 1) % len(ck.servers)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// 仅当请求中的版本与服务器上该键的版本匹配时，才使用 value 更新键。
// 如果版本号不匹配，服务器应返回 ErrVersion。
// 如果 Put 在第一次 RPC 时收到 ErrVersion，Put 应返回 ErrVersion，
// 因为服务器上肯定没有执行 Put。
// 如果服务器在重发 RPC 时返回 ErrVersion，那么 Put 必须向应用返回 ErrMaybe，
// 因为早先的 RPC 可能已被服务器成功处理，但响应丢失，
// Clerk 无法确定 Put 是否已经执行。
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}

	hasSentAtLeastOne := false

	for {
		index := ck.lastLeader
		for i := 0; i < len(ck.servers); i++ {

			var reply rpc.PutReply
			ok := ck.clnt.Call(ck.servers[index], "KVServer.Put", &args, &reply)

			if ok {
				if reply.Err == rpc.ErrWrongLeader {
					index = (index + 1) % len(ck.servers)
					continue
				}
				ck.lastLeader = index
				// 核心判断
				if reply.Err == rpc.ErrVersion && hasSentAtLeastOne {
					return rpc.ErrMaybe
				}else if reply.Err == rpc.ErrVersion && !hasSentAtLeastOne{
					return rpc.ErrVersion
				}
				
				return reply.Err
			}
			index = (index + 1) % len(ck.servers)
		}
		hasSentAtLeastOne = true
		time.Sleep(50 * time.Millisecond)
	}
}
