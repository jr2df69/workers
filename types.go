package workers

import (
	"errors"

	"github.com/sirupsen/logrus"
)

// ErrWaitingWorkerBusy - worker is busy
var (
	ErrWorkerBusy = errors.New("Worker is already busy")
	ErrEmptyJob   = errors.New("job not defined")
)

// OnStartFunc - function that called on start
// is returns ErrWaitingWorkerBusy - worker already busy
type OnStartFunc func() error

func dummyOnStart() error { return nil }

// OnFinishedFunc - function that called on finish
type OnFinishedFunc func()

func dummyOnFinished() {}

// RunnerFunc - job function
type RunnerFunc func(logger *logrus.Entry)
