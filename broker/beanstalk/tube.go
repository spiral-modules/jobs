package beanstalk

import (
	"encoding/json"
	"errors"
	"github.com/beanstalkd/go-beanstalk"
	"github.com/spiral/jobs"
	"github.com/spiral/jobs/cpool"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type tube struct {
	active     int32
	pipe       *jobs.Pipeline
	mut        sync.Mutex
	tube       *beanstalk.Tube
	tubeSet    *beanstalk.TubeSet
	connPool   *cpool.ConnPool
	reserve    time.Duration
	touch      time.Duration
	cmdTimeout time.Duration
	lsn        func(event int, ctx interface{})
	wait       chan interface{}
	waitTouch  chan interface{}
	wg         sync.WaitGroup
	execPool   chan jobs.Handler
	err        jobs.ErrorHandler
	mur        sync.Mutex
	touchJobs  map[uint64]bool
}

// create new tube consumer and producer
func newTube(
	pipe *jobs.Pipeline,
	connPool *cpool.ConnPool,
	reserve time.Duration,
	touch time.Duration,
	cmdTimeout time.Duration,
	listener func(event int, ctx interface{}),
) (*tube, error) {
	if pipe.String("tube", "") == "" {
		return nil, errors.New("missing `tube` parameter on beanstalk pipeline")
	}

	return &tube{
		pipe:       pipe,
		tube:       &beanstalk.Tube{Name: pipe.String("tube", "")},
		tubeSet:    beanstalk.NewTubeSet(nil, pipe.String("tube", "")),
		connPool:   connPool,
		reserve:    reserve,
		touch:      touch,
		cmdTimeout: cmdTimeout,
		lsn:        listener,
		touchJobs:  make(map[uint64]bool),
	}, nil
}

// associate tube with new consume pool
func (t *tube) configure(execPool chan jobs.Handler, err jobs.ErrorHandler) error {
	t.execPool = execPool
	t.err = err

	return nil
}

// run consumers
func (t *tube) serve() {
	t.wait = make(chan interface{})
	t.waitTouch = make(chan interface{})
	atomic.StoreInt32(&t.active, 1)

	go t.serveTouch()

	for {
		select {
		case <-t.wait:
			return
		default:
			t.wg.Add(1)
			t.connPool.Exec(t.consume, t.cmdTimeout)
			t.wg.Done()
		}
	}
}

// manage expiration of jobs which are currently being processed.
func (t *tube) serveTouch() {
	if t.touch == 0 {
		return
	}

	touch := time.NewTicker(t.touch)
	defer touch.Stop()

	for {
		select {
		case <-t.waitTouch:
			return
		case <-touch.C:
			t.mur.Lock()
			touch := make([]uint64, 0, len(t.touchJobs))
			for id := range t.touchJobs {
				touch = append(touch, id)
			}
			t.mur.Unlock()

			t.wg.Add(len(touch))

			for _, id := range touch {
				err := t.connPool.Exec(func(c interface{}) error {
					return wrapErr(c.(*beanstalk.Conn).Touch(id), false)
				}, t.cmdTimeout)

				t.wg.Done()

				if err != nil {
					t.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: t.pipe, Caused: err})
				}
			}
		}
	}
}

// stop tube consuming
func (t *tube) stop() {
	if atomic.LoadInt32(&t.active) == 0 {
		return
	}

	atomic.StoreInt32(&t.active, 0)

	close(t.wait)
	t.wg.Wait()
	close(t.waitTouch)
}

// put data into pool or return error (no wait)
func (t *tube) put(data []byte, attempt int, delay, rrt time.Duration) (id string, err error) {
	t.wg.Add(1)
	defer t.wg.Done()

	var bid uint64

	// execute operation on first free conn
	err = t.connPool.Exec(func(c interface{}) error {
		t.mut.Lock()
		defer t.mut.Unlock()

		t.tube.Conn = c.(*beanstalk.Conn)

		bid, err = t.tube.Put(data, 0, delay, rrt)
		return wrapErr(err, false)
	}, t.cmdTimeout)

	return jid(bid), err
}

// return tube stats
func (t *tube) stat() (stat *jobs.Stat, err error) {
	t.wg.Add(1)
	defer t.wg.Done()

	values := make(map[string]string)

	err = t.connPool.Exec(func(c interface{}) error {
		t.mut.Lock()
		defer t.mut.Unlock()

		t.tube.Conn = c.(*beanstalk.Conn)

		values, err = t.tube.Stats()
		return wrapErr(err, false)
	}, t.cmdTimeout)

	if err != nil {
		return nil, err
	}

	stat = &jobs.Stat{InternalName: t.tube.Name}

	if v, err := strconv.Atoi(values["current-jobs-ready"]); err == nil {
		stat.Queue = int64(v)
	}

	if v, err := strconv.Atoi(values["current-jobs-touch"]); err == nil {
		stat.Active = int64(v)
	}

	if v, err := strconv.Atoi(values["current-jobs-delayed"]); err == nil {
		stat.Delayed = int64(v)
	}

	return stat, err
}

// consume job
func (t *tube) consume(c interface{}) error {
	t.tubeSet.Conn = c.(*beanstalk.Conn)

	id, body, err := t.tubeSet.Reserve(t.reserve)
	if err != nil {
		// pass only conn errors
		return wrapErr(err, true)
	}

	var j *jobs.Job
	err = json.Unmarshal(body, &j)

	// to avoid job expiration until the handler is found
	t.reserveJob(id)

	if err != nil {
		t.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: t.pipe, Caused: err})
		return nil
	}

	t.wg.Add(1)
	go func(h jobs.Handler, c *beanstalk.Conn) {
		defer t.wg.Done()

		err = h(jid(id), j)
		t.execPool <- h

		t.releaseJob(id)

		if err == nil {
			if err := c.Delete(id); err != nil {
				t.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: t.pipe, Caused: err})
			}
			return
		}

		// number of reserves
		stat, _ := c.StatsJob(id)
		reserves, _ := strconv.Atoi(stat["reserves"])

		if j.Options.CanRetry(reserves) {
			// retrying
			if err := c.Release(id, 0, j.Options.RetryDuration()); err != nil {
				t.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: t.pipe, Caused: err})
			}
			return
		}

		t.err(jid(id), j, err)
		if err := c.Bury(id, 0); err != nil {
			t.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: t.pipe, Caused: err})
		}
	}(<-t.execPool, t.tubeSet.Conn)

	return nil
}

// reserve job (to be touched every reserve/2 intervals
func (t *tube) reserveJob(id uint64) {
	t.mur.Lock()
	defer t.mur.Unlock()

	t.touchJobs[id] = true
}

// release releases the job
func (t *tube) releaseJob(id uint64) {
	t.mur.Lock()
	defer t.mur.Unlock()

	delete(t.touchJobs, id)
}

// throw handles service, server and pool events.
func (t *tube) throw(event int, ctx interface{}) {
	t.lsn(event, ctx)
}

// jid converts job id into string.
func jid(id uint64) string {
	if id == 0 {
		return ""
	}
	return strconv.FormatUint(id, 10)
}

// wrapError into conn error when detected. softErr would not wrap any of no connection errors.
func wrapErr(err error, hide bool) error {
	if err == nil {
		return nil
	}

	if cpool.IsConnError(err) {
		return cpool.ConnError{Err: err}
	}

	if hide {
		return nil
	}

	return err
}
