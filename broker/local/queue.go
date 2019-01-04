package local

import (
	"github.com/pkg/errors"
	"github.com/spiral/jobs"
	"sync"
	"sync/atomic"
	"time"
)

type queue struct {
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

func newQueue() *queue {
	return &queue{stat: &jobs.Stat{}, jobs: make(chan entry)}
}

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

func (q *queue) configure(execPool chan jobs.Handler, err jobs.ErrorHandler) error {
	if q.wait != nil {
		return errors.New("unable to configure active queue")
	}

	q.execPool = execPool
	q.err = err

	return nil
}

func (q *queue) serve() {
	if q.wait != nil || q.execPool == nil {
		// already wait or no need to run
		return
	}

	// we are wait now
	q.wait = make(chan interface{})

	var h jobs.Handler
	for {
		select {
		case <-q.wait:
			q.wg.Wait()
			return
		case e := <-q.jobs:
			q.wg.Add(1)
			atomic.AddInt64(&q.stat.Active, 1)

			// wait for free h
			h = <-q.execPool

			go func(e entry, handler jobs.Handler) {
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
			}(e, h)
		}
	}
}

func (q *queue) stop() {
	if q.wait == nil {
		// not wait
		return
	}

	close(q.wait)
	q.wait = nil
}
