package ephemeral

import (
	"github.com/spiral/jobs"
	"sync"
	"sync/atomic"
	"time"
)

type queue struct {
	active int32
	st     *jobs.Stat

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
	return &queue{st: &jobs.Stat{}, jobs: make(chan *entry)}
}

// associate queue with new do pool
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
		e := q.consume()
		if e == nil {
			return
		}

		atomic.AddInt64(&q.st.Active, 1)
		h := <-q.execPool
		go func(e *entry) {
			q.do(h, e)
			atomic.AddInt64(&q.st.Active, ^int64(0))
			q.execPool <- h
			q.wg.Done()
		}(e)
	}
}

// allocate one job entry
func (q *queue) consume() *entry {
	q.muw.Lock()
	defer q.muw.Unlock()

	select {
	case <-q.wait:
		return nil
	case e := <-q.jobs:
		q.wg.Add(1)

		return e
	}
}

// do singe job
func (q *queue) do(h jobs.Handler, e *entry) {
	err := h(e.id, e.job)

	if err == nil {
		atomic.AddInt64(&q.st.Queue, ^int64(0))
		return
	}

	q.err(e.id, e.job, err)

	e.attempt++
	if !e.job.Options.CanRetry(e.attempt) {
		atomic.AddInt64(&q.st.Queue, ^int64(0))
		return
	}

	q.push(e.id, e.job, e.attempt, e.job.Options.RetryDuration())
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
		atomic.AddInt64(&q.st.Queue, 1)
		go func() {
			q.jobs <- &entry{id: id, job: j, attempt: attempt}
		}()

		return
	}

	atomic.AddInt64(&q.st.Delayed, 1)
	go func() {
		time.Sleep(delay)
		atomic.AddInt64(&q.st.Delayed, ^int64(0))
		atomic.AddInt64(&q.st.Queue, 1)

		q.jobs <- &entry{id: id, job: j, attempt: attempt}
	}()
}

func (q *queue) stat() *jobs.Stat {
	return &jobs.Stat{
		InternalName: ":memory:",
		Queue:        atomic.LoadInt64(&q.st.Queue),
		Active:       atomic.LoadInt64(&q.st.Active),
		Delayed:      atomic.LoadInt64(&q.st.Delayed),
	}
}
