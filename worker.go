package workers

import (
	"context"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

// AsyncJobWorkerOptions - async on-demand job worker
type AsyncJobWorkerOptions struct {
	ParallelWorkersCount int
}

// Job - job interface
type Job interface {
	OnStart() error
	Run(ctx context.Context)
	OnFinish()
}

// JobWorker - worker
type JobWorker interface {
	RunWith(ctx context.Context, job Job) error
	CurrentJob() Job

	Running() bool
	StartedAt() time.Time
	FinishedAt() time.Time
	Stop()
}

// NewAsyncJobWorker - new async worker for on-demand job
func NewAsyncJobWorker(logger *logrus.Logger, opts *AsyncJobWorkerOptions) JobWorker {
	return newAsyncJobWorker(logger, opts)
}

func newAsyncJobWorker(logger *logrus.Logger, opts *AsyncJobWorkerOptions) *asyncJobWorker {
	ajw := &asyncJobWorker{
		logger: logger,

		opts: opts,
	}

	if ajw.logger == nil {
		logger = logrus.StandardLogger()
	}

	return ajw
}

type asyncJobWorker struct {
	mutex      sync.Mutex
	currentJob Job

	logger *logrus.Logger

	opts *AsyncJobWorkerOptions

	running bool

	startedAt  time.Time
	finishedAt time.Time

	cFuncs []context.CancelFunc
}

func (ajw *asyncJobWorker) Running() bool {
	ajw.mutex.Lock()
	defer ajw.mutex.Unlock()
	return ajw.running
}

func (ajw *asyncJobWorker) StartedAt() time.Time {
	ajw.mutex.Lock()
	defer ajw.mutex.Unlock()
	return ajw.startedAt
}

func (ajw *asyncJobWorker) FinishedAt() time.Time {
	ajw.mutex.Lock()
	defer ajw.mutex.Unlock()
	return ajw.finishedAt
}

func (ajw *asyncJobWorker) CurrentJob() Job {
	return ajw.currentJob
}

func (ajw *asyncJobWorker) RunWith(ctx context.Context, job Job) error {
	ajw.mutex.Lock()
	if ajw.running {
		ajw.mutex.Unlock()
		return ErrWorkerBusy
	}

	err := ajw.setRunning(job)
	if err != nil {
		return err
	}

	ajw.cFuncs = make([]context.CancelFunc, 0)

	wg := &sync.WaitGroup{}
	for i := 0; i < ajw.opts.ParallelWorkersCount; i++ {
		wg.Add(1)
		ctx, cFunc := context.WithCancel(ctx)
		ajw.cFuncs = append(ajw.cFuncs, cFunc)
		go ajw.runner(ctx, wg, job)
	}

	ajw.mutex.Unlock()

	go ajw.waitAndFinish(wg, job)

	return nil
}

func (ajw *asyncJobWorker) setRunning(job Job) error {
	err := job.OnStart()
	if err != nil {
		return err
	}

	ajw.currentJob = job
	ajw.running = true
	ajw.startedAt = time.Now()

	return nil
}

func (ajw *asyncJobWorker) setFinished(job Job) {
	ajw.currentJob = nil
	ajw.running = false
	ajw.finishedAt = time.Now()
	job.OnFinish()
}

func (ajw *asyncJobWorker) waitAndFinish(wg *sync.WaitGroup, job Job) {
	wg.Wait()
	ajw.mutex.Lock()
	ajw.setFinished(job)
	ajw.mutex.Unlock()
}

// runner - async worker subworker
func (ajw *asyncJobWorker) runner(ctx context.Context, wg *sync.WaitGroup, job Job) {
	workerLogger := ajw.logger.WithField("logger_id", uuid.NewV4().String())
	workerLogger.Warn("worker started")
	defer wg.Done()

	job.Run(ctx)

	workerLogger.Warn("worker stopped")
}

func (ajw *asyncJobWorker) Stop() {
	for _, cFunc := range ajw.cFuncs {
		if cFunc != nil {
			cFunc()
		}
	}

	ajw.cFuncs = make([]context.CancelFunc, 0)
}
