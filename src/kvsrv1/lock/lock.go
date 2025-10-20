package lock

import (
	"fmt"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	lockName string
	clientId string
	// You may add code here
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{
		ck:       ck,
		lockName: l,
		clientId: kvtest.RandValue(8),
	}
	return lk
}
func (lk *Lock) Acquire() {
	fmt.Println("acquire()1")
	for {
		val, ver, err := lk.ck.Get(lk.lockName)

		if err == rpc.ErrNoKey {
			putErr := lk.ck.Put(lk.lockName, "", 0)
			if putErr != rpc.OK && putErr != rpc.ErrMaybe {
				time.Sleep(10 * time.Millisecond)
			}
			continue
		} else if err == rpc.ErrVersion {
			continue
		}

		if val == "" {
			err2 := lk.ck.Put(lk.lockName, lk.clientId, ver)
			if err2 == rpc.OK || err2 == rpc.ErrMaybe {
				val2, _, err3 := lk.ck.Get(lk.lockName)
				if err3 == rpc.OK && val2 == lk.clientId {
					fmt.Println("acquire()2")
					return
				} else if err3 == rpc.ErrMaybe {
					val2, _, err3 := lk.ck.Get(lk.lockName)
					if (err3 == rpc.OK || err3 == rpc.ErrMaybe) && val2 == lk.clientId {
						return
					}
					continue
				}
			}
			continue
		}
		time.Sleep(10 * time.Millisecond)
	}
}
func (lk *Lock) Release() {
	fmt.Println("release()1")
	for {
		val, ver, err := lk.ck.Get(lk.lockName)
		if err != rpc.OK && err != rpc.ErrMaybe {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if val == lk.clientId {
			putErr := lk.ck.Put(lk.lockName, "", ver)
			if putErr == rpc.OK || putErr == rpc.ErrMaybe {
				val2, _, err2 := lk.ck.Get(lk.lockName)
				if (err2 == rpc.OK || err2 == rpc.ErrMaybe) && val2 == "" {
					fmt.Println("release()2")
					return
				}
			}else if putErr == rpc.ErrVersion{
				continue
			}
		} else {
			fmt.Println("release()2 val != lk.clientId")
			return
		}

		time.Sleep(10 * time.Millisecond)
	}
}
