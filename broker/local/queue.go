package local

import (
	"github.com/spiral/jobs"
	"sync"
	"sync/atomic"
	"time"
)

type queue struct {
	active   int32
	stat     *jobs.Stat
	jobs     chan entry
	wg       sync.WaitGroup
	wait     chan interface{}
	execPool chan jobs.Handler
	err      jobs.ErrorHandler
}

type entry struct {
	id      string
	job     *jobs.Job
	attempt int
}

// create new queue
func newQueue() *queue {
	return &queue{stat: &jobs.Stat{}, jobs: make(chan entry)}
}

// add job to the queue
func (q *queue) push(id string, j *jobs.Job, attempt int, delay time.Duration) {
	if delay == 0 {
		atomic.AddInt64(&q.stat.Queue, 1)
		q.jobs <- entry{id: id, job: j}
		return
	}

	atomic.AddInt64(&q.stat.Delayed, 1)

	time.Sleep(delay)

	atomic.AddInt64(&q.stat.Delayed, ^int64(0))
	atomic.AddInt64(&q.stat.Queue, 1)

	q.jobs <- entry{id: id, job: j, attempt: attempt}
}

// associate queue with new exec pool
func (q *queue) configure(execPool chan jobs.Handler, err jobs.ErrorHandler) error {
	q.execPool = execPool
	q.err = err

	return nil
}

// serve consumers
func (q *queue) serve() {
	q.wait = make(chan interface{})
	atomic.StoreInt32(&q.active, 1)

	for {
		select {
		case <-q.wait:
			return
		case e := <-q.jobs:
			q.wg.Add(1)
			atomic.AddInt64(&q.stat.Active, 1)

			go q.exec(e, <-q.execPool)
		}
	}
}

// stop the queue consuming
func (q *queue) stop() {
	if atomic.LoadInt32(&q.active) == 0 {
		return
	}

	atomic.StoreInt32(&q.active, 0)

	close(q.wait)
	q.wg.Wait()
}

// exec singe job
func (q *queue) exec(e entry, handler jobs.Handler) {
	defer atomic.AddInt64(&q.stat.Active, ^int64(0))
	defer q.wg.Done()

	e.attempt++

	err := handler(e.id, e.job)
	q.execPool <- handler

	if err == nil {
		atomic.AddInt64(&q.stat.Queue, ^int64(0))
		return
	}

	if e.job.Options.CanRetry(e.attempt) {
		q.push(e.id, e.job, e.attempt, e.job.Options.RetryDuration())
		return
	}

	q.err(e.id, e.job, err)
	atomic.AddInt64(&q.stat.Queue, ^int64(0))
}
