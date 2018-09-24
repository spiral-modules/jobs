package local

import (
	"github.com/satori/go.uuid"
	"github.com/spiral/jobs"
	"sync"
	"time"
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
func (b *Broker) Listen(pipelines []*jobs.Pipeline, exe jobs.Handler, err jobs.ErrorHandler) error {
	b.exe = exe
	b.err = err
	return nil
}

// Init configures local job broker.
func (b *Broker) Init(cfg *Config) (bool, error) {
	if err := cfg.Valid(); err != nil {
		return false, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.cfg = cfg
	b.queue = make(chan entry)

	return true, nil
}

// Serve local broker.
func (b *Broker) Serve() error {
	for i := 0; i < b.cfg.Threads; i++ {
		b.wg.Add(1)
		go b.listen()
	}

	b.wg.Wait()
	return nil
}

// Stop local broker.
func (b *Broker) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.queue != nil {
		close(b.queue)
	}
}

// Push new job to queue
func (b *Broker) Push(p *jobs.Pipeline, j *jobs.Job) (string, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return "", err
	}

	// todo: handle stop

	go func() { b.queue <- entry{id: id.String(), job: j} }()

	return id.String(), nil
}

func (b *Broker) listen() {
	defer b.wg.Done()
	for q := range b.queue {
		id, job := q.id, q.job

		if job.Options.Delay != 0 {
			time.Sleep(job.Options.DelayDuration())
		}

		// todo: move to goroutine

		// local broker does not support job timeouts yet
		err := b.exe(id, job)
		if err == nil {
			continue
		}

		if !job.CanRetry() {
			b.err(id, job, err)
			continue
		}

		if job.Options.RetryDelay != 0 {
			time.Sleep(job.Options.RetryDuration())
		}

		b.queue <- entry{id: id, job: job}
	}
}
