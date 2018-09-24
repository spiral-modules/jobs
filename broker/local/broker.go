package local

import (
	"github.com/satori/go.uuid"
	"github.com/spiral/jobs"
	"sync"
	"time"
)

// Broker run queue using local goroutines.
type Broker struct {
	mu          sync.Mutex
	wg          sync.WaitGroup
	queue       chan entry
	handlerPool chan jobs.Handler
	err         jobs.ErrorHandler
}

type entry struct {
	id  string
	job *jobs.Job
}

// Listen configures broker with list of pipelines to listen and handler function. Broker broker groups all pipelines
// together.
func (b *Broker) Listen(pipelines []*jobs.Pipeline, pool chan jobs.Handler, err jobs.ErrorHandler) error {
	b.handlerPool = pool
	b.err = err
	return nil
}

// Init configures local job broker.
func (b *Broker) Init() (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.queue = make(chan entry)
	return true, nil
}

// Serve local broker.
func (b *Broker) Serve() error {
	b.mu.Lock()
	b.queue = make(chan entry)
	b.mu.Unlock()

	var handler jobs.Handler
	for q := range b.queue {
		id, job := q.id, q.job

		if job.Options.Delay != 0 {
			time.Sleep(job.Options.DelayDuration())
		}

		// wait for free handler
		handler = <-b.handlerPool

		go func() {
			err := handler(id, job)
			b.handlerPool <- handler

			if err == nil {
				return
			}

			if !job.CanRetry() {
				b.err(id, job, err)
				return
			}

			if job.Options.RetryDelay != 0 {
				time.Sleep(job.Options.RetryDuration())
			}

			b.queue <- entry{id: id, job: job}
		}()
	}

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

	go func() { b.queue <- entry{id: id.String(), job: j} }()

	return id.String(), nil
}
