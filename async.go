package workers

import (
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

	wg := &sync.WaitGroup{}
	for i := 0; i < aw.options.ParallelWorkersCount; i++ {
		go aw.runner(wg)
		wg.Add(1)
	}
	aw.logger.Warn("workers started")

	wg.Wait()

	aw.job.OnFinish()
	aw.logger.Warn("workers finished")

	return nil
}

// runner - async worker subworker
func (aw *asyncWorker) runner(wg *sync.WaitGroup) {
	workerLogger := aw.logger.WithField("logger_id", uuid.NewV4().String())
	workerLogger.Warn("worker started")
	defer wg.Done()

	aw.job.Run(workerLogger)

	workerLogger.Warn("worker stopped")
}
