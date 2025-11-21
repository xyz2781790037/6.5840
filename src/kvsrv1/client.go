package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
	"time"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.

	return ck
}

// Get 获取键的当前值和版本。如果键不存在，则返回 ErrNoKey。面对所有其他错误，它将永远尝试。
// 你可以发送一个带有如下代码的 RPC：
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
// args 和 reply 的类型（包括它们是否为指针）必须与 RPC 处理函数声明的参数类型匹配。此外，reply 必须作为指针传递。

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}

	for {
		reply := rpc.GetReply{}
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
		if ok {
			return reply.Value, reply.Version, reply.Err
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	is_retry := false
	// 第一次 Put 出现 ErrVersion，说明你确实传错版本，应该返回真正的错误；

	// 重试时 出现 ErrVersion，说明上次可能已经成功，只是响应丢了。
	for {
		reply := rpc.PutReply{}

		ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
		if ok {
			if is_retry && reply.Err == rpc.ErrVersion {
				return rpc.ErrMaybe
			}
			return reply.Err
		} else {
			time.Sleep(50 * time.Millisecond)
		}
		is_retry = true
	}
}
