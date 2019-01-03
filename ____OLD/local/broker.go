package local

import (
	"github.com/satori/go.uuid"
	"github.com/spiral/jobs"
	"sync"
	"sync/atomic"
	"time"
)

// Broker run queue using local goroutines.
type Broker struct {
	status   int32
	mu       sync.Mutex
	wg       sync.WaitGroup
	queue    chan entry
	stat     *jobs.Stat
	execPool chan jobs.Handler
	err      jobs.ErrorHandler
}

type entry struct {
	id      string
	attempt int
	job     *jobs.Job
}

// Status returns broken status.
func (b *Broker) Status() jobs.BrokerStatus {
	return jobs.BrokerStatus(atomic.LoadInt32(&b.status))
}

// Listen configures broker with list of pipelines to listen and handler function. Broker broker groups all pipelines
// together.
func (b *Broker) Listen(pipelines []*jobs.Pipeline, execPool chan jobs.Handler, err jobs.ErrorHandler) error {
	b.execPool = execPool
	b.err = err
	return nil
}

// Init configures local job broker.
func (b *Broker) Init() (bool, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.queue = make(chan entry)
	b.stat = &jobs.Stat{Pipeline: ":memory:"}

	return true, nil
}

// serve local broker.
func (b *Broker) Serve() error {
	b.mu.Lock()
	b.queue = make(chan entry)
	b.mu.Unlock()

	// ready to accept jobs
	atomic.StoreInt32(&b.status, int32(jobs.StatusReady))

	var h jobs.Handler
	for e := range b.queue {
		b.wg.Add(1)

		// wait for free h
		h = <-b.execPool

		atomic.AddInt64(&b.stat.Active, 1)
		go func(e entry, handler jobs.Handler) {
			defer atomic.AddInt64(&b.stat.Active, ^int64(0))
			defer b.wg.Done()

			e.attempt++

			err := handler(e.id, e.job)
			b.execPool <- handler

			if err == nil {
				atomic.AddInt64(&b.stat.Queue, ^int64(0))
				return
			}

			if e.job.CanRetry(e.attempt) {
				b.schedule(e.id, e.attempt, e.job, e.job.Options.RetryDuration())
				return
			}

			b.err(e.id, e.job, err)
			atomic.AddInt64(&b.stat.Queue, ^int64(0))
		}(e, h)
	}

	b.wg.Wait()
	return nil
}

// stop local broker.
func (b *Broker) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()

	atomic.StoreInt32(&b.status, int32(jobs.StatusRegistered))

	if b.queue != nil {
		close(b.queue)
		b.queue = nil
	}
}

// Push new job to queue
func (b *Broker) Push(p *jobs.Pipeline, j *jobs.Job) (string, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return "", err
	}

	go b.schedule(id.String(), 0, j, j.Options.DelayDuration())
	return id.String(), nil
}

// Stat must fetch statistics about given pipeline or return error.
func (b *Broker) Stat(p *jobs.Pipeline) (stat *jobs.Stat, err error) {
	return b.stat, nil
}

// addJob adds job to queue
func (b *Broker) schedule(id string, attempt int, j *jobs.Job, delay time.Duration) {
	if delay == 0 {
		atomic.AddInt64(&b.stat.Queue, 1)
		b.queue <- entry{id: id, job: j}
		return
	}

	atomic.AddInt64(&b.stat.Delayed, 1)

	time.Sleep(delay)

	atomic.AddInt64(&b.stat.Delayed, ^int64(0))
	atomic.AddInt64(&b.stat.Queue, 1)

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.queue != nil {
		b.queue <- entry{id: id, attempt: attempt, job: j}
	}
}
