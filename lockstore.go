package main

import (
	"container/heap"
	"errors"
	"fmt"
	"sync"
)

var (
	errInProgress = errors.New("azkaban: work on resource in progress")
	errFinalized  = errors.New("azkaban: work on resource completed")
	errCapacity   = errors.New("azkaban: the store is at capacity")
)

//This locking system will be used to assure exactly once completion of a work item.
//Locking occurs in two phases: a "work in progress" phase, where other workers may not start an item that has been already aquired,
//but should still store the item for retries until either the original worker has completed the item, or has failed to check in.
//The second phase is a "work completed" phase, where other workers should mark the work as already completed.
type LockStorer interface {
	InProgressLock(resource, owner string, expire, now int64) (int64, error)
	CompletedLock(resource, owner string, expire, now int64) error
}

//A Lock consists of the owner of a lock, the resource being locked, a time at which the lock expires, and a bool thats true when in a finalized state.
type Lock struct {
	Owner      string
	Resource   string
	ExpireTime int64
	Completed  bool
	Index      int
}

//Allow us to keep track of our object sizes, to prevent from overflowing memory.
//Size of the struct is 8 bytes per string, 8 bytes for an int64, 4 bytes for an int, and 4 bytes for a bool.
//Plus we add 8 bytes for "resource" as a key in the resourceMap, and 4 bytes for the key in the indexMap.
func (l *Lock) Size() int {
	return 44 + len(l.Resource) + len(l.Owner)
}

//The EvictionLockStorer maintains a history of temporary and permanent locks.
//Both tempoary and permanent locks have a specified expiration time.
//If capacity of the store is violated, we return an error.
//The structure is designed for synchronous access, but protects itself with a lock.
type EvictionLockStorer struct {
	capacity    int
	size        int
	count       int
	lock        sync.RWMutex
	resourceMap map[string]*Lock
	indexMap    map[int]*Lock
}

//Since implementing the heap interface is something we don't want publically exposed, we return it as the LockStorer interface.
//We are using a hashheap instead of the slice heap, to cut down on the overhead of appending in the reference implementation.
func NewLockStorer(capacity int) LockStorer {
	store := &EvictionLockStorer{
		capacity:    capacity,
		size:        0,
		resourceMap: make(map[string]*Lock),
		indexMap:    make(map[int]*Lock),
	}
	heap.Init(store)
	return store
}

//This temporary lock must be called prior to beginning work on an item.
//If the temporary lock has been acquired by another owner, it will return errInProgress, until that lease is expired.
//If the finalized lock has been acquired by another owner, it will return errFinalized.
//Temporary locks can be retriable.  Finalized locks should not.
//Operations are O(log n), where n is the number of keys in the store.
func (els *EvictionLockStorer) InProgressLock(resource, owner string, expire, now int64) (int64, error) {
	els.lock.Lock()
	defer els.lock.Unlock()

	els.EvictItems(now)

	existingItem, ok := els.resourceMap[resource]
	if ok {
		if existingItem.Completed {
			return 0, errFinalized
		}
		if existingItem.Owner == owner {
			els.update(existingItem, existingItem.Resource, existingItem.Owner, false, expire)
			return expire, nil
		}
		return existingItem.ExpireTime, errInProgress
	} else {
		newItem := &Lock{
			Resource:   resource,
			Owner:      owner,
			ExpireTime: expire,
			Completed:  false,
		}

		if newItem.Size()+els.size > els.capacity {
			return 0, errCapacity
		}

		heap.Push(els, newItem)
		return newItem.ExpireTime, nil
	}
}

//This permanent lock must be called after completing work on an item.
//If the temporary lock has been acquired by another owner, it will override that lock with the permanent lock.
//If the finalized lock has been acquired by another owner, it will return errFinalized.
//Operations are O(log n), where n is the number of keys in the store.
func (els *EvictionLockStorer) CompletedLock(resource, owner string, expire, now int64) error {
	els.lock.Lock()
	defer els.lock.Unlock()

	els.EvictItems(now)
	existingItem, ok := els.resourceMap[resource]
	if ok {
		if existingItem.Completed {
			return errFinalized
		}
		els.update(existingItem, existingItem.Resource, existingItem.Owner, true, expire)
		return nil
	} else {
		newItem := &Lock{
			Resource:   resource,
			Owner:      owner,
			ExpireTime: expire,
			Completed:  true,
		}

		if newItem.Size()+els.size > els.capacity {
			return errCapacity
		}

		heap.Push(els, newItem)
		return nil
	}
}

func (els *EvictionLockStorer) EvictItems(now int64) {
	for {
		item := els.peek()
		if item == nil || item.ExpireTime > now {
			return
		}
		fmt.Println("Evict", item)
		_ = heap.Pop(els)
		els.count -= 1
		delete(els.resourceMap, item.Resource)
	}
}

//Number of entries in the lock store.
//Required to implement the sort interface
func (els *EvictionLockStorer) Len() int {
	return els.count
}

//Determines which of two locks will expire first.
//Required to implement the sort interface
func (els *EvictionLockStorer) Less(i, j int) bool {
	return els.indexMap[i].ExpireTime < els.indexMap[j].ExpireTime
}

//Swaps the position in the hashheap of two items.
//Required to implement the sort interface
func (els *EvictionLockStorer) Swap(i, j int) {
	item1 := els.indexMap[i]
	item2 := els.indexMap[j]
	item2.Index = i
	item1.Index = j
	els.indexMap[i] = item2
	els.indexMap[j] = item1
}

//Adds a new item to the heap.
//Required to implement the container.heap interface
func (els *EvictionLockStorer) Push(x interface{}) {
	n := els.count
	item := x.(*Lock)
	item.Index = n
	els.resourceMap[item.Resource] = item
	els.indexMap[n] = item
	els.count += 1
}

//Removes the item with the oldest expire time from the heap.
//Required to implement the container.heap interface
func (els *EvictionLockStorer) Pop() interface{} {
	n := els.count
	item := els.indexMap[n-1]
	item.Index = -1
	delete(els.indexMap, n-1)
	delete(els.resourceMap, item.Resource)
	els.count -= 1
	els.size -= item.Size()
	return item
}

//We need to be able to peek items to see if they should be expired
func (els *EvictionLockStorer) peek() *Lock {
	n := len(els.indexMap)
	if n == 0 {
		return nil
	}
	item := els.indexMap[n-1]
	return item
}

//Updates an item's position in the hashheap.
//Items in the middle of the heap may have their expire time updated
func (els *EvictionLockStorer) update(item *Lock, resource, owner string, completed bool, expire int64) {
	els.size -= item.Size()
	item.Resource = resource
	item.Owner = owner
	item.ExpireTime = expire
	els.size += item.Size()
	heap.Fix(els, item.Index)
}
