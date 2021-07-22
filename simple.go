package workers

import (
	"errors"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

type simpleWorker struct {
	*commonWaitingWorker

	onStartFunc    OnStartFunc
	onFinishedFunc OnFinishedFunc
	runnerFunc     RunnerFunc
}

// SimpleOptions - simple worker options
type SimpleOptions struct {
	SleepTimeout time.Duration
	RunOnLoad    bool
}

// NewSimpleWorker - initializes new simple worker
func NewSimpleWorker(
	logger *logrus.Logger,
	onStartFunc OnStartFunc,
	onFinishedFunc OnFinishedFunc,
	runnerFunc RunnerFunc,
	o *SimpleOptions,
) WaitingWorker {
	return newSimpleWorker(logger, onStartFunc, onFinishedFunc, runnerFunc, o)
}

func newSimpleWorker(
	logger *logrus.Logger,
	onStartFunc OnStartFunc,
	onFinishedFunc OnFinishedFunc,
	runnerFunc RunnerFunc,
	o *SimpleOptions,
) *simpleWorker {
	sw := &simpleWorker{
		commonWaitingWorker: newCommon(
			logger,
			o.RunOnLoad,
			o.SleepTimeout,
		),

		onStartFunc:    onStartFunc,
		onFinishedFunc: onFinishedFunc,
		runnerFunc:     runnerFunc,
	}

	if sw.onStartFunc == nil {
		sw.onStartFunc = dummyOnStart
	}
	if sw.onFinishedFunc == nil {
		sw.onFinishedFunc = dummyOnFinished
	}

	sw.commonWaitingWorker.workerFunc = sw.startWork

	return sw
}

// startWork - starting simple worker work
func (sw *simpleWorker) startWork() {
	workerLogger := sw.logger.WithField("logger_id", uuid.NewV4().String())
	workerLogger.Info("worker starting")

	if sw.runnerFunc == nil {
		panic(errors.New("runner func is not defined"))
	}

	sw.onStartFunc()
	sw.runnerFunc(workerLogger)
	sw.onFinishedFunc()

	workerLogger.Info("worker finished")
}
