package workers

import (
	"errors"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

type asyncTestConfig struct {
	errOnStart   error
	opts         *AsyncOptions
	mustBeForced bool
	iterations   int
}

func TestAsyncWorkerByTimeout(t *testing.T) {
	test := &asyncTestConfig{
		opts: &AsyncOptions{
			ParallelWorkersCount: 10,
			SleepTimeout:         2 * time.Second,
		},
	}

	testAsyncWorker(test, t)
}

func TestAsyncWorkerOnLoad(t *testing.T) {
	test := &asyncTestConfig{
		opts: &AsyncOptions{
			ParallelWorkersCount: 20,
			SleepTimeout:         2 * time.Second,
			RunOnLoad:            true,
		},
	}

	testAsyncWorker(test, t)
}

func TestAsyncWorkerForced(t *testing.T) {
	test := &asyncTestConfig{
		opts: &AsyncOptions{
			ParallelWorkersCount: 5,
			SleepTimeout:         2 * time.Hour,
			RunOnLoad:            false,
		},
		mustBeForced: true,
	}

	testAsyncWorker(test, t)
}

func TestAsyncWorkerIteratively(t *testing.T) {
	test := &asyncTestConfig{
		opts: &AsyncOptions{
			ParallelWorkersCount: 10,
			SleepTimeout:         2 * time.Second,
		},
		iterations: 3,
	}

	testAsyncWorker(test, t)
}

func TestAsyncWorkerStartError(t *testing.T) {
	test := &asyncTestConfig{
		opts: &AsyncOptions{
			ParallelWorkersCount: 5,
			SleepTimeout:         1 * time.Second,
			RunOnLoad:            false,
		},
		errOnStart: errors.New("some start error"),
	}

	testAsyncWorker(test, t)
}

func testAsyncWorker(test *asyncTestConfig, t *testing.T) {
	buf := test.opts.ParallelWorkersCount + 2 //старт и стоп
	ast := newWorkerTester(buf, test.errOnStart)

	logger := logrus.StandardLogger()
	logger.SetLevel(logrus.DebugLevel)
	w := newWaitingAsync(logger, ast, test.opts)

	if test.iterations <= 1 {
		testWorker(t, w, ast, test.opts.ParallelWorkersCount, test.mustBeForced, test.opts.RunOnLoad, test.opts.SleepTimeout, test.errOnStart)
		return
	}

	for i := 0; i < test.iterations; i++ {
		t.Logf("Running iteration %d", i+1)
		testWorker(t, w, ast, test.opts.ParallelWorkersCount, test.mustBeForced, test.opts.RunOnLoad, test.opts.SleepTimeout, test.errOnStart)
	}
}
