package lock

import (
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
	key      string
	clientID string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, key: l}
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	clientID := kvtest.RandValue(8)
	lk.clientID = clientID

	for {
		val, version, err := lk.ck.Get(lk.key)
		if err == rpc.OK {
			if val == "FREE" {
				ok := lk.ck.Put(lk.key, clientID, version)
				if ok == rpc.OK {
					return
				}
				if ok == rpc.ErrMaybe {
					val, _, _ := lk.ck.Get(lk.key)
					if val == clientID {
						return
					}
				}
			}
		} else {
			if err == rpc.ErrNoKey {
				ok := lk.ck.Put(lk.key, clientID, 0)
				if ok == rpc.OK {
					return
				}
				if ok == rpc.ErrMaybe {
					val, _, _ := lk.ck.Get(lk.key)
					if val == clientID {
						return
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

}

func (lk *Lock) Release() {
	// Your code here
	for {
		val, version, err := lk.ck.Get(lk.key)

		if err != rpc.OK {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		if val != lk.clientID {
			return
		}

		ok := lk.ck.Put(lk.key, "FREE", version)
		if ok == rpc.OK {
			return
		}
		if ok == rpc.ErrMaybe {
			val, _, _ := lk.ck.Get(lk.key)
			if val == "FREE" {
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
