package workers

import (
	"errors"
	"io"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// WaitingWorker - worker interface
type WaitingWorker interface {
	io.Closer
	Wait()
	Force() error
	LastError() error

	Running() bool
	StartedAt() time.Time
	FinishedAt() time.Time
}

// commonWaitingWorker - common worker model
type commonWaitingWorker struct {
	mutex       *sync.Mutex
	stopCh      chan bool
	stoppedCh   chan bool
	forceSyncCh chan bool

	logger *logrus.Logger

	running    bool
	startedAt  time.Time
	finishedAt time.Time

	workerFunc func() error

	lastError error

	runOnLoad    bool
	sleepTimeout time.Duration
}

// newCommon - new common waiting worker
func newCommon(
	logger *logrus.Logger,
	runOnLoad bool,
	sleepTimeout time.Duration,
) *commonWaitingWorker {
	w := &commonWaitingWorker{
		mutex: &sync.Mutex{},

		stopCh:      make(chan bool),
		stoppedCh:   make(chan bool),
		forceSyncCh: make(chan bool),

		logger: logger,

		runOnLoad:    runOnLoad,
		sleepTimeout: sleepTimeout,
	}

	if w.logger == nil {
		w.logger = logrus.StandardLogger()
	}

	return w
}

// Wait - waiting for signal or time elapsed
func (w *commonWaitingWorker) Wait() {
	w.logger.WithField("timeout", w.sleepTimeout).WithField("runOnLoad", w.runOnLoad).Warn("running periodically sync with timeout")

	if w.runOnLoad {
		w.startWork()
	}

	ticker := time.NewTicker(w.sleepTimeout)
	for {
		select {
		case <-ticker.C:
			w.startWork()
			ticker.Reset(w.sleepTimeout)
		case <-w.forceSyncCh:
			w.startWork()
			ticker.Reset(w.sleepTimeout)
		case <-w.stopCh:
			ticker.Stop()
			w.stoppedCh <- true
			return
		}
	}
}

// Close - stops worker
func (w *commonWaitingWorker) Close() error {
	w.stopCh <- true
	<-w.stoppedCh
	return nil
}

// Force - forces worker
func (w *commonWaitingWorker) Force() error {
	select {
	case w.forceSyncCh <- true:
	case <-time.After(10 * time.Millisecond):
		return ErrWorkerBusy
	}

	return nil
}

// Running - returns worker status thread-safely
func (w *commonWaitingWorker) Running() bool {
	return w.running
}

// StartedAt - returns time when worker was started last time
func (w *commonWaitingWorker) StartedAt() time.Time {
	return w.startedAt
}

func (w *commonWaitingWorker) FinishedAt() time.Time {
	return w.finishedAt
}

// startWork - starts job. If nothing to start, it panics
func (w *commonWaitingWorker) startWork() {
	if w.workerFunc == nil {
		panic(errors.New("common worker couldn't work. It's just a model"))
	}

	w.setRunning(true)
	defer w.setRunning(false)
	if err := w.workerFunc(); err != nil {
		w.logger.WithError(err).Error("unable to start job")
		w.lastError = err
		return
	}
}

func (w *commonWaitingWorker) storeError(err error) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.lastError = err
}

func (w *commonWaitingWorker) LastError() error {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.lastError
}

// setRunning - sets worker status thread-safely
func (w *commonWaitingWorker) setRunning(running bool) {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	w.running = running
	if running {
		w.startedAt = time.Now()
	} else {
		w.finishedAt = time.Now()
	}
}
