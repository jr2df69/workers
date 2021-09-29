package workers

import (
	"context"
	"testing"
	"time"
)

type workerTester struct {
	eventsCh   chan string
	errOnStart error

	buf int
}

const (
	eventRunner       = "runner"
	eventOnStart      = "onStart"
	eventOnStartError = "onStartError"
	eventOnFinish     = "onFinish"
	eventAbort        = "abort"
)

func newWorkerTester(buf int, errOnStart error) *workerTester {
	return &workerTester{
		buf:        buf,
		errOnStart: errOnStart,
	}
}

func (wt *workerTester) OnStart() error {
	wt.eventsCh = make(chan string)

	evt := eventOnStartError
	if wt.errOnStart == nil {
		evt = eventOnStart
	}
	select {
	case wt.eventsCh <- evt:
		/*case <-time.After(1 * time.Second):
		panic(errors.New("too long"))*/
	}

	return wt.errOnStart
}

func (wt *workerTester) Run(ctx context.Context) {
	select {
	case <-ctx.Done():
	case wt.eventsCh <- eventRunner:
		/*case <-time.After(1 * time.Second):
		panic(errors.New("too long"))*/
	}
}

func (wt *workerTester) OnFinish() {
	select {
	case wt.eventsCh <- eventOnFinish:
		/*	case <-time.After(1 * time.Second):
			panic(errors.New("too long"))*/
	}

	close(wt.eventsCh)
}

func (wt *workerTester) events() <-chan string {
	return wt.eventsCh
}

func testWorker(t *testing.T, w WaitingWorker, tester *workerTester, parallelsCount int, mustBeForced bool, runOnLoad bool, sleepTimeout time.Duration, errOnStart error) {
	defer func() {
		err := w.Close()
		if err != nil {
			t.Fatalf("error on closing (must be without error): %s", err)
		}
	}()

	go w.Wait()
	if mustBeForced {
		err := w.Force()
		if err != nil {
			t.Fatalf("unexpected error on force: %s", err.Error())
		}
	} else if !runOnLoad {
		time.Sleep(sleepTimeout)
	}

	wasStart := false
	wasFinish := false
	runnerEventsCount := 0
	time.Sleep(500 * time.Millisecond)
	eventsCh := tester.events()

check:
	for evt := range eventsCh {
		switch evt {
		case eventOnStart:
			if wasStart {
				t.Fatal("on start event duplicated")
			}
			if wasFinish {
				t.Fatal("on start after on finish")
			}
			if runnerEventsCount > 0 {
				t.Fatal("on start after runner")
			}
			wasStart = true
		case eventOnStartError:
			if wasStart {
				t.Fatal("on start error event duplicated")
			}
			if wasFinish {
				t.Fatal("on start error after on finish")
			}
			if runnerEventsCount > 0 {
				t.Fatal("on start errors after runner")
			}
			if errOnStart == nil {
				t.Fatal("got on start error when must not!")
			}
			t.Log("test passed after on start error")
			return
		case eventOnFinish:
			if !wasStart {
				t.Fatal("on finish event before start")
			}
			if runnerEventsCount <= 0 {
				t.Fatal("runner events missing before finish")
			}
			if wasFinish {
				t.Fatal("on finish event duplicated")
			}
			wasFinish = true
			break check
		case eventRunner:
			runnerEventsCount++
		default:
			t.Fatalf("unknown event: %s", evt)
		}
	}

	if !wasStart {
		t.Fatal("missing start event")
	}

	if !wasFinish {
		t.Fatal("missing finish event")
	}

	if runnerEventsCount != parallelsCount {
		t.Fatalf("runner events mismatch: wanted %d, got %d", parallelsCount, runnerEventsCount)
	}
}
