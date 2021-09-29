package workers

import (
	"context"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

type simpleWorker struct {
	*commonWaitingWorker

	job Job

	cancelFunc context.CancelFunc
}

// SimpleOptions - simple worker options
type SimpleOptions struct {
	SleepTimeout time.Duration
	RunOnLoad    bool
}

// NewSimpleWorker - initializes new simple worker
func NewSimpleWorker(
	logger *logrus.Logger,
	job Job,
	o *SimpleOptions,
) WaitingWorker {
	return newSimpleWorker(logger, job, o)
}

func newSimpleWorker(
	logger *logrus.Logger,
	job Job,
	o *SimpleOptions,
) *simpleWorker {
	sw := &simpleWorker{
		commonWaitingWorker: newCommon(
			logger,
			o.RunOnLoad,
			o.SleepTimeout,
		),

		job: job,
	}

	sw.commonWaitingWorker.workerFunc = sw.startWork

	return sw
}

// startWork - starting simple worker work
func (sw *simpleWorker) startWork() error {
	workerLogger := sw.logger.WithField("logger_id", uuid.NewV4().String())
	workerLogger.Info("worker starting")

	if sw.job == nil {
		return ErrEmptyJob
	}

	if err := sw.job.OnStart(); err != nil {
		return err
	}

	var ctx context.Context
	ctx, sw.cancelFunc = context.WithCancel(context.Background())

	sw.job.Run(ctx)
	sw.job.OnFinish()

	workerLogger.Info("worker finished")

	return nil
}

func (sw *simpleWorker) Stop() {
	if sw.cancelFunc != nil {
		sw.cancelFunc()
		sw.cancelFunc = nil
	}
}
