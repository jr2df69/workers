package workers

import (
	"errors"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

// AsyncOptions - async worker options
type AsyncOptions struct {
	ParallelWorkersCount int
	SleepTimeout         time.Duration
	RunOnLoad            bool
}

type asyncWorker struct {
	*commonWaitingWorker
	options *AsyncOptions

	onStartFunc    OnStartFunc
	onFinishedFunc OnFinishedFunc
	runnerFunc     RunnerFunc
}

func NewAsync(
	logger *logrus.Logger,
	onStartFunc OnStartFunc,
	onFinishedFunc OnFinishedFunc,
	runnerFunc RunnerFunc,
	o *AsyncOptions,
) WaitingWorker {
	return newAsync(logger, onStartFunc, onFinishedFunc, runnerFunc, o)
}

func newAsync(logger *logrus.Logger,
	onStartFunc OnStartFunc,
	onFinishedFunc OnFinishedFunc,
	runnerFunc RunnerFunc,
	o *AsyncOptions,
) *asyncWorker {
	aw := &asyncWorker{
		options: o,
		commonWaitingWorker: newCommon(
			logger,
			o.RunOnLoad,
			o.SleepTimeout,
		),

		onStartFunc:    onStartFunc,
		onFinishedFunc: onFinishedFunc,
		runnerFunc:     runnerFunc,
	}

	if aw.onStartFunc == nil {
		aw.onStartFunc = dummyOnStart
	}
	if aw.onFinishedFunc == nil {
		aw.onFinishedFunc = dummyOnFinished
	}

	aw.commonWaitingWorker.workerFunc = aw.startWork

	return aw
}

// startWork - starts async worker
func (aw *asyncWorker) startWork() {
	if aw.runnerFunc == nil {
		panic(errors.New("runner func is not defined"))
	}

	err := aw.onStartFunc()
	if err != nil {
		aw.logger.WithError(err).Error("starting worker was aborted by error")
		return
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < aw.options.ParallelWorkersCount; i++ {
		go aw.runner(wg)
		wg.Add(1)
	}
	aw.logger.Warn("workers started")

	wg.Wait()

	aw.onFinishedFunc()

	aw.logger.Warn("workers finished")
}

// runner - async worker subworker
func (aw *asyncWorker) runner(wg *sync.WaitGroup) {
	workerLogger := aw.logger.WithField("logger_id", uuid.NewV4().String())
	workerLogger.Warn("worker started")
	defer wg.Done()

	aw.runnerFunc(workerLogger)

	workerLogger.Warn("worker stopped")
}
