package testutils

import "sync"

func RunAndWait(fn func()) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		fn()
		wg.Done()
	}()
	wg.Wait()
}
