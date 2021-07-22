package workers

import (
	"errors"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

type jobWorkerTest struct {
	workerOpts *AsyncJobWorkerOptions
	testJob    *testJob
}

type testJob struct {
	jobChan      chan string
	onStartError error
}

func (tj *testJob) Run(_ *logrus.Entry) {
	tj.jobChan <- eventRunner
}

func (tj *testJob) onStart() error {
	if tj.onStartError == nil {
		tj.jobChan <- eventOnStart
		return nil
	}

	tj.jobChan <- eventOnStartError
	return tj.onStartError
}

func (tj *testJob) onFinish() {
	tj.jobChan <- eventOnFinish
	close(tj.jobChan)
}

func TestJobWorkAsync(t *testing.T) {
	jwt := &jobWorkerTest{
		workerOpts: &AsyncJobWorkerOptions{
			ParallelWorkersCount: 10,
		},
		testJob: &testJob{
			jobChan: make(chan string, 12),
		},
	}

	testAsyncJobWorker(jwt, t)
}

func TestJobWorkSingle(t *testing.T) {
	jwt := &jobWorkerTest{
		workerOpts: &AsyncJobWorkerOptions{
			ParallelWorkersCount: 1,
		},
		testJob: &testJob{
			jobChan: make(chan string, 3),
		},
	}

	testAsyncJobWorker(jwt, t)
}

func TestJobWorkOnStartError(t *testing.T) {
	jwt := &jobWorkerTest{
		workerOpts: &AsyncJobWorkerOptions{
			ParallelWorkersCount: 1,
		},
		testJob: &testJob{
			jobChan:      make(chan string, 3),
			onStartError: errors.New("some on start error"),
		},
	}

	testAsyncJobWorker(jwt, t)
}

func testAsyncJobWorker(test *jobWorkerTest, t *testing.T) {
	logrus.SetLevel(logrus.DebugLevel)
	worker := newAsyncJobWorker(test.testJob.onStart, test.testJob.onFinish, logrus.StandardLogger(), test.workerOpts)

	err := worker.RunWith(test.testJob)
	if err != test.testJob.onStartError {
		t.Fatalf("unexpected error on start: %s", err.Error())
	} else if err != nil {
		t.Logf("returned start error: %s", err.Error())
		return
	}

	wasStart := false
	wasFinish := false
	runnerEventsCount := 0
check:
	for {
		eventsCh := test.testJob.jobChan
		select {
		case evt := <-eventsCh:
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
				if test.testJob.onStartError == nil {
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
		case <-time.After(50 * time.Millisecond):
			t.Fatalf("too slow")
		}
	}

	if !wasStart {
		t.Fatal("missing start event")
	}

	if !wasFinish {
		t.Fatal("missing finish event")
	}

	if runnerEventsCount != test.workerOpts.ParallelWorkersCount {
		t.Fatalf("runner events mismatch: wanted %d, got %d", test.workerOpts.ParallelWorkersCount, runnerEventsCount)
	}

	if worker.StartedAt().IsZero() {
		t.Fatalf("missing started at info")
	}

	if worker.finishedAt.IsZero() {
		t.Fatal("missing finished at info")
	}
}
