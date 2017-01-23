// This example demonstrates a priority queue built using the heap interface.
package main

import (
	"fmt"
)

// This example of lock storage pattern.
func main() {
	ls := NewLockStorer(1024)
	expire, err := ls.InProgressLock("item1", "worker1", 5, 0)
	fmt.Println(expire, err)

	err = ls.CompletedLock("item1", "worker1", 100, 0)
	fmt.Println(err)
}
