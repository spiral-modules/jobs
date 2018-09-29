package local

import (
	"github.com/satori/go.uuid"
	"github.com/spiral/jobs"
	"sync"
	"time"
	"sync/atomic"
)

// Broker run queue using local goroutines.
type Broker struct {
	mu          sync.Mutex
	wg          sync.WaitGroup
	stopped     bool
	queue       chan entry
	handlerPool chan jobs.Handler
	err         jobs.ErrorHandler
	stat        *jobs.PipelineStat
}

type entry struct {
	id      string
	attempt int
	job     *jobs.Job
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
	b.stat = &jobs.PipelineStat{Pipeline: "in-memory"}

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
			atomic.AddInt64(&b.stat.Delayed, ^int64(0))
		}

		// wait for free handler
		handler = <-b.handlerPool

		atomic.AddInt64(&b.stat.Active, 1)
		go func() {
			defer atomic.AddInt64(&b.stat.Active, ^int64(0))

			err := handler(id, job)
			b.handlerPool <- handler

			if err == nil {
				atomic.AddInt64(&b.stat.Pending, ^int64(0))
				return
			}

			if !job.CanRetry() {
				b.err(id, job, err)
				atomic.AddInt64(&b.stat.Pending, ^int64(0))
				return
			}

			if job.Options.RetryDelay != 0 {
				atomic.AddInt64(&b.stat.Delayed, 1)
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

	if !b.stopped {
		//todo: send to close channel
		close(b.queue)
		b.stopped = true
	}
}

// Push new job to queue
func (b *Broker) Push(p *jobs.Pipeline, j *jobs.Job) (string, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return "", err
	}

	if j.Options.Delay != 0 {
		atomic.AddInt64(&b.stat.Delayed, 1)
	} else {
		atomic.AddInt64(&b.stat.Pending, 1)
	}

	go func() { b.queue <- entry{id: id.String(), job: j} }()

	return id.String(), nil
}

// Stat must fetch statistics about given pipeline or return error.
func (b *Broker) Stat(p *jobs.Pipeline) (stat *jobs.PipelineStat, err error) {
	return b.stat, nil
}
