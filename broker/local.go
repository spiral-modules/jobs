package broker

import (
	"github.com/spiral/jobs"
	"sync"
	"time"
	"errors"
	"github.com/satori/go.uuid"
)

// Local run jobs using local goroutines.
type Local struct {
	mu      sync.Mutex
	threads int
	wg      sync.WaitGroup
	jobs    chan entry
	exec    jobs.Handler
	error   jobs.ErrorHandler
}

type entry struct {
	id  string
	job *jobs.Job
}

// Init configures local job broker.
func (l *Local) Init() (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.jobs = make(chan entry)

	return true, nil
}

// Handle configures broker with list of pipelines to listen and handler function. Local broker groups all pipelines
// together.
func (l *Local) Handle(pipelines []*jobs.Pipeline, h jobs.Handler, f jobs.ErrorHandler) error {
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

	l.exec = h
	l.error = f
	return nil
}

// Serve local broker.
func (l *Local) Serve() error {
	for i := 0; i < l.threads; i++ {
		l.wg.Add(1)
		go l.listen()
	}

	l.wg.Wait()
	return nil
}

// Stop local broker.
func (l *Local) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.jobs != nil {
		close(l.jobs)
		l.jobs = nil
	}
}

// Push new job to queue
func (l *Local) Push(p *jobs.Pipeline, j *jobs.Job) (string, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return "", err
	}

	go func() { l.jobs <- entry{id: id.String(), job: j} }()

	return id.String(), nil
}

func (l *Local) listen() {
	defer l.wg.Done()
	for j := range l.jobs {
		id, job := j.id, j.job

		if job.Options.Delay != 0 {
			time.Sleep(job.Options.DelayDuration())
		}

		// local broker does not have a way to confirm job re-execution
		if err := l.exec(id, job); err != nil {
			if job.CanRetry() {
				l.jobs <- entry{id: id, job: job}
			} else {
				l.error(id, job, err)
			}
		}
	}
}
