package testutils

import "sync"

// RunAndWait runs the provided function in a go-routine and waits until it returns
// It's assumed that there is some mechanism by which fn returns that's outside the scope of this function.
func RunAndWait(fn func()) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		fn()
		wg.Done()
	}()
	wg.Wait()
}
