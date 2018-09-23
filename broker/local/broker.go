package local

import (
	"github.com/spiral/jobs"
	"sync"
	"time"
	"github.com/satori/go.uuid"
)

// Broker run queue using local goroutines.
type Broker struct {
	mu    sync.Mutex
	cfg   *Config
	wg    sync.WaitGroup
	queue chan entry
	exe   jobs.Handler
	err   jobs.ErrorHandler
}

type entry struct {
	id  string
	job *jobs.Job
}

// Listen configures broker with list of pipelines to listen and handler function. Broker broker groups all pipelines
// together.
func (l *Broker) Listen(pipelines []*jobs.Pipeline, exe jobs.Handler, err jobs.ErrorHandler) error {
	l.exe = exe
	l.err = err
	return nil
}

// Init configures local job broker.
func (l *Broker) Init(cfg *Config) (bool, error) {
	if err := cfg.Valid(); err != nil {
		return false, err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	l.cfg = cfg
	l.queue = make(chan entry)

	return true, nil
}

// Serve local broker.
func (l *Broker) Serve() error {
	for i := 0; i < l.cfg.Threads; i++ {
		l.wg.Add(1)
		go l.listen()
	}

	l.wg.Wait()
	return nil
}

// Stop local broker.
func (l *Broker) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.queue != nil {
		close(l.queue)
	}
}

// Push new job to queue
func (l *Broker) Push(p *jobs.Pipeline, j *jobs.Job) (string, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return "", err
	}

	// todo: handle stop

	go func() { l.queue <- entry{id: id.String(), job: j} }()

	return id.String(), nil
}

func (l *Broker) listen() {
	defer l.wg.Done()
	for q := range l.queue {
		id, job := q.id, q.job

		if job.Options.Delay != 0 {
			time.Sleep(job.Options.DelayDuration())
		}

		// local broker does not support job timeouts yet
		err := l.exe(id, job)
		if err == nil {
			continue
		}

		if !job.CanRetry() {
			l.err(id, job, err)
			continue
		}

		if job.Options.RetryDelay != 0 {
			time.Sleep(job.Options.RetryDuration())
		}

		l.queue <- entry{id: id, job: job}
	}
}
