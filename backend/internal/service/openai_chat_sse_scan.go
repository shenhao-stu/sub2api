package service

import (
	"bufio"
	"time"
)

// Only the caller invokes onLine/onIdle, so all response writes have one owner.
// The reader is bounded and remains attached until EOF or the existing drain
// deadline closes its body, retaining late authoritative usage after disconnect.
func scanOpenAIChatLines(scanner *bufio.Scanner, interval time.Duration, onLine func(string), onIdle func()) error {
	if interval <= 0 {
		for scanner.Scan() {
			onLine(scanner.Text())
		}
		return scanner.Err()
	}
	type event struct {
		line string
		err  error
		done bool
	}
	events := make(chan event, 1)
	done := make(chan struct{})
	defer close(done)
	send := func(next event) bool {
		select {
		case events <- next:
			return true
		case <-done:
			return false
		}
	}
	go func() {
		for scanner.Scan() {
			if !send(event{line: scanner.Text()}) {
				return
			}
		}
		send(event{err: scanner.Err(), done: true})
	}()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case next := <-events:
			if next.done {
				return next.err
			}
			onLine(next.line)
		case <-ticker.C:
			onIdle()
		}
	}
}
