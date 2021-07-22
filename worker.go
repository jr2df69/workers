package workers

import (
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
)

type AsyncJobWorkerOptions struct {
	ParallelWorkersCount int
}

type Job interface {
	OnStart() error
	Run(*logrus.Entry)
	OnFinish()
}

type JobWorker interface {
	RunWith(job Job) error
	CurrentJob() Job

	Running() bool
	StartedAt() time.Time
	FinishedAt() time.Time
}

func NewAsyncJobWorker(onStart OnStartFunc, onFinished OnFinishedFunc, logger *logrus.Logger, opts *AsyncJobWorkerOptions) JobWorker {
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

func (ajw *asyncJobWorker) RunWith(job Job) error {
	ajw.mutex.Lock()
	if ajw.running {
		ajw.mutex.Unlock()
		return ErrWorkerBusy
	}

	err := ajw.setRunning(job)
	if err != nil {
		return err
	}

	wg := &sync.WaitGroup{}
	for i := 0; i < ajw.opts.ParallelWorkersCount; i++ {
		wg.Add(1)
		go ajw.runner(wg, job)
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
func (ajw *asyncJobWorker) runner(wg *sync.WaitGroup, job Job) {
	workerLogger := ajw.logger.WithField("logger_id", uuid.NewV4().String())
	workerLogger.Warn("worker started")
	defer wg.Done()

	job.Run(workerLogger)

	workerLogger.Warn("worker stopped")
}
