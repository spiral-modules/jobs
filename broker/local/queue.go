package local

import (
	"github.com/spiral/jobs"
	"sync"
	"sync/atomic"
	"time"
)

type queue struct {
	active int32
	stat   *jobs.Stat

	// job pipeline
	jobs chan *entry

	// active operations
	muw sync.Mutex
	wg  sync.WaitGroup

	// stop channel
	wait chan interface{}

	// exec handlers
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
	return &queue{stat: &jobs.Stat{}, jobs: make(chan *entry)}
}

// associate queue with new consume pool
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
		e := q.allocateEntry()
		if e == nil {
			return
		}

		go q.consume(e, <-q.execPool)
	}
}

// allocate one job entry
func (q *queue) allocateEntry() *entry {
	q.muw.Lock()
	defer q.muw.Unlock()

	select {
	case <-q.wait:
		return nil
	case e := <-q.jobs:
		atomic.AddInt64(&q.stat.Active, 1)
		q.wg.Add(1)

		return e
	}
}

// stop the queue consuming
func (q *queue) stop() {
	if atomic.LoadInt32(&q.active) == 0 {
		return
	}

	atomic.StoreInt32(&q.active, 0)

	close(q.wait)
	q.muw.Lock()
	q.wg.Wait()
	q.muw.Unlock()
}

// add job to the queue
func (q *queue) push(id string, j *jobs.Job, attempt int, delay time.Duration) {
	if delay == 0 {
		atomic.AddInt64(&q.stat.Queue, 1)
		q.jobs <- &entry{id: id, job: j}
		return
	}

	atomic.AddInt64(&q.stat.Delayed, 1)

	time.Sleep(delay)

	atomic.AddInt64(&q.stat.Delayed, ^int64(0))
	atomic.AddInt64(&q.stat.Queue, 1)

	q.jobs <- &entry{id: id, job: j, attempt: attempt}
}

// consume singe job
func (q *queue) consume(e *entry, handler jobs.Handler) {
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
