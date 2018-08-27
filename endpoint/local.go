package endpoint

import (
	"errors"
	"github.com/spiral/jobs"
	"sync"
	"time"
)

// Local run jobs using local goroutines.
type Local struct {
	mu   sync.Mutex
	jobs chan *jobs.Job
}

// Push new job to queue
func (l *Local) Push(j *jobs.Job) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.jobs == nil {
		return errors.New("local job endpoint is not started")
	}

	l.jobs <- j
	return nil
}

// Serve local endpoint.
func (l *Local) Serve(svc *jobs.Service) error {
	l.mu.Lock()
	l.jobs = make(chan *jobs.Job)
	l.mu.Unlock()

	for j := range l.jobs {
		go func(j *jobs.Job) {
			if j.Options != nil && j.Options.Delay != nil {
				time.Sleep(time.Second * time.Duration(*j.Options.Delay))
			}

			svc.Exec(j)
		}(j)
	}

	return nil
}

// Stop local endpoint.
func (l *Local) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.jobs != nil {
		close(l.jobs)
		l.jobs = nil
	}
}
