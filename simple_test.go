package workers

import (
	"errors"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
)

type simpleTestConfig struct {
	errOnStart   error
	opts         *SimpleOptions
	mustBeForced bool
	iterations   int
}

func TestSimpleWorkerByTimeout(t *testing.T) {
	test := &simpleTestConfig{
		opts: &SimpleOptions{
			SleepTimeout: 2 * time.Second,
		},
	}

	testSimpleWorker(test, t)
}

func TestSimpleWorkerByTimeoutIteratively(t *testing.T) {
	test := &simpleTestConfig{
		opts: &SimpleOptions{
			SleepTimeout: 1 * time.Second,
		},
		iterations: 3,
	}

	testSimpleWorker(test, t)
}

func TestSimpleWorkerOnLoad(t *testing.T) {
	test := &simpleTestConfig{
		opts: &SimpleOptions{
			SleepTimeout: 2 * time.Second,
			RunOnLoad:    true,
		},
	}

	testSimpleWorker(test, t)
}

func TestSimpleWorkerForced(t *testing.T) {
	test := &simpleTestConfig{
		opts: &SimpleOptions{
			SleepTimeout: 2 * time.Hour,
			RunOnLoad:    false,
		},
		mustBeForced: true,
	}

	testSimpleWorker(test, t)
}

func TestSimpleWorkerStartError(t *testing.T) {
	test := &simpleTestConfig{
		opts: &SimpleOptions{
			SleepTimeout: 2 * time.Second,
			RunOnLoad:    false,
		},
		errOnStart: errors.New("some start error"),
	}

	testSimpleWorker(test, t)
}

func testSimpleWorker(test *simpleTestConfig, t *testing.T) {
	buf := 3 //старт, runner и стоп
	ast := newWorkerTester(buf, test.errOnStart)

	logger := logrus.StandardLogger()
	logger.SetLevel(logrus.DebugLevel)
	w := newSimpleWorker(logger, ast, test.opts)
	if test.iterations <= 1 {
		testWorker(t, w, ast, 1, test.mustBeForced, test.opts.RunOnLoad, test.opts.SleepTimeout, test.errOnStart)
		return
	}

	for i := 0; i < test.iterations; i++ {
		t.Logf("Running iteration %d", i+1)
		testWorker(t, w, ast, 1, test.mustBeForced, test.opts.RunOnLoad, test.opts.SleepTimeout, test.errOnStart)
	}
}
