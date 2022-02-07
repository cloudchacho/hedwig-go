package testutils

import (
	"time"
)

// RunAndWait runs the provided function in a go-routine and waits until it returns
// or until a timeout hits.
// It's assumed that there is some mechanism by which fn returns that's outside the scope of this function.
// If the timeout hits, that's a failure condition since the goroutine should've stopped.
func RunAndWait(fn func()) {
	ch := make(chan struct{})
	go func() {
		fn()
		ch <- struct{}{}
	}()
	timer := time.NewTimer(time.Second * 2)
	defer timer.Stop()
	select {
	case <-timer.C:
		panic("goroutine never returned")
	case <-ch:
		return
	}
}
