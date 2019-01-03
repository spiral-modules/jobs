package local

import (
	"github.com/spiral/jobs"
	"sync"
	"sync/atomic"
	"time"
)

type queue struct {
	jobs     chan entry
	mu       sync.Mutex
	wg       sync.WaitGroup
	active   int32
	stopped  chan interface{}
	execPool chan jobs.Handler
	err      jobs.ErrorHandler
}

type entry struct {
	id      string
	job     *jobs.Job
	attempt int
}

func newQueue() *queue {
	return &queue{jobs: make(chan entry)}
}

func (q *queue) push(id string, j *jobs.Job, attempt int, delay time.Duration) {
	// todo: what about delay?

	q.jobs <- entry{id: id, job: j, attempt: attempt}
}

func (q *queue) configure(execPool chan jobs.Handler, err jobs.ErrorHandler) error {
	q.mu.Lock()
	defer q.mu.Unlock()

	q.execPool = execPool
	q.err = err

	return nil
}

func (q *queue) stop() {
	if atomic.LoadInt32(&q.active) == 0 {
		// not stopped
		return
	}

	atomic.StoreInt32(&q.active, 0)

	close(q.stopped)
	q.wg.Wait()
}

func (q *queue) serve() {
	if atomic.LoadInt32(&q.active) == 1 || q.execPool == nil {
		// already stopped or no need to run
		return
	}

	// we are stopped now
	q.mu.Lock()
	q.stopped = make(chan interface{})
	q.mu.Unlock()

	atomic.StoreInt32(&q.active, 1)

	var h jobs.Handler
	for {
		select {
		case <-q.stopped:
			atomic.StoreInt32(&q.active, 0)
			q.wg.Wait()
			return
		case e := <-q.jobs:
			q.wg.Add(1)

			// wait for free h
			h = <-q.execPool

			// atomic.AddInt64(&b.stat.Active, 1)
			go func(e entry, handler jobs.Handler) {
				// defer atomic.AddInt64(&b.stat.Active, ^int64(0))
				defer q.wg.Done()

				e.attempt++

				err := handler(e.id, e.job)
				q.execPool <- handler

				if err == nil {
					// atomic.AddInt64(&b.stat.Queue, ^int64(0))
					return
				}

				if e.job.Options.CanRetry(e.attempt) {
					q.push(e.id, e.job, e.attempt, e.job.Options.RetryDuration())
					return
				}

				q.err(e.id, e.job, err)
				// atomic.AddInt64(&b.stat.Queue, ^int64(0))
			}(e, h)
		}
	}
}
