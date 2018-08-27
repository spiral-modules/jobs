package endpoint

import (
	"github.com/spiral/jobs"
	"sync"
	"time"
)

// Local run jobs using local goroutines.
type Local struct {
	mu   sync.Mutex
	jobs chan *jobs.Job
	exec jobs.Handler
}

// SetHandler configures function to handle job execution.
func (l *Local) Handler(exec jobs.Handler) {
	l.exec = exec
}

// Init configures local job endpoint.
func (l *Local) Init() (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.jobs = make(chan *jobs.Job)

	return true, nil
}

// Push new job to queue
func (l *Local) Push(j *jobs.Job) error {
	l.jobs <- j
	return nil
}

// Serve local endpoint.
func (l *Local) Serve() error {
	for j := range l.jobs {
		go func(j *jobs.Job) {
			if j.Options != nil && j.Options.Delay != nil {
				time.Sleep(time.Second * time.Duration(*j.Options.Delay))
			}

			l.exec(j)
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
