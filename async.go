package workers

import (
	"context"
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

	job Job

	cancelFuncs []context.CancelFunc
}

// NewWaitingAsync - new waiting async job worker
func NewWaitingAsync(
	logger *logrus.Logger,
	job Job,
	o *AsyncOptions,
) WaitingWorker {
	return newWaitingAsync(logger, job, o)
}

func newWaitingAsync(logger *logrus.Logger,
	job Job,
	o *AsyncOptions,
) *asyncWorker {
	aw := &asyncWorker{
		options: o,
		commonWaitingWorker: newCommon(
			logger,
			o.RunOnLoad,
			o.SleepTimeout,
		),

		job: job,
	}

	aw.commonWaitingWorker.workerFunc = aw.startWork

	return aw
}

// startWork - starts async worker
func (aw *asyncWorker) startWork() error {
	if aw.job == nil {
		return ErrEmptyJob
	}

	err := aw.job.OnStart()
	if err != nil {
		return err
	}
	aw.cancelFuncs = make([]context.CancelFunc, 0, aw.options.ParallelWorkersCount)

	wg := &sync.WaitGroup{}
	for i := 0; i < aw.options.ParallelWorkersCount; i++ {
		ctx, cFunc := context.WithCancel(context.Background())
		go aw.runner(ctx, wg)
		aw.cancelFuncs = append(aw.cancelFuncs, cFunc)
		wg.Add(1)
	}
	aw.logger.Warn("workers started")

	wg.Wait()

	aw.job.OnFinish()
	aw.logger.Warn("workers finished")

	return nil
}

// runner - async worker subworker
func (aw *asyncWorker) runner(ctx context.Context, wg *sync.WaitGroup) {
	workerLogger := aw.logger.WithField("logger_id", uuid.NewV4().String())
	workerLogger.Warn("worker started")
	defer wg.Done()

	aw.job.Run(ctx)

	workerLogger.Warn("worker stopped")
}

func (aw *asyncWorker) Stop() {
	for _, cFunc := range aw.cancelFuncs {
		if cFunc != nil {
			cFunc()
		}
	}
	aw.cancelFuncs = make([]context.CancelFunc, 0)
}
