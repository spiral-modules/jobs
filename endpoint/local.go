package endpoint

import (
	"github.com/spiral/jobs"
	"sync"
	"time"
	"errors"
)

// Local run jobs using local goroutines.
type Local struct {
	mu      sync.Mutex
	threads int
	wg      sync.WaitGroup
	jobs    chan *jobs.Job
	exec    jobs.Handler
}

// Handle configures endpoint with list of pipelines to listen and handler function. Local endpoint groups all pipelines
// together.
func (l *Local) Handle(pipelines []*jobs.Pipeline, exec jobs.Handler) error {
	switch {
	case len(pipelines) < 1:
		// no pipelines to handleThread
		return nil

	case len(pipelines) == 1:
		l.threads = pipelines[0].Options.Integer("threads", 1)
		if l.threads < 1 {
			return errors.New("local queue handler threads must be 1 or higher")
		}

	default:
		return errors.New("local queue handler expects exactly one pipeline")
	}

	l.exec = exec
	return nil
}

// Init configures local job endpoint.
func (l *Local) Init() (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.jobs = make(chan *jobs.Job)

	return true, nil
}

// Push new job to queue
func (l *Local) Push(p *jobs.Pipeline, j *jobs.Job) error {
	l.jobs <- j
	return nil
}

// Serve local endpoint.
func (l *Local) Serve() error {
	for i := 0; i < l.threads; i++ {
		l.wg.Add(1)
		go l.listen()
	}

	l.wg.Wait()
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

func (l *Local) listen() {
	defer l.wg.Done()
	for j := range l.jobs {
		if j == nil {
			return
		}

		if j.Options != nil && j.Options.Delay != nil {
			time.Sleep(time.Second * time.Duration(*j.Options.Delay))
		}

		l.exec(j)
	}
}
