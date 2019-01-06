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
	active   int32
	pipe     *jobs.Pipeline
	mut      sync.Mutex
	tube     *beanstalk.Tube
	tubeSet  *beanstalk.TubeSet
	connPool *cpool.ConnPool
	reserve  time.Duration
	lsn      func(event int, ctx interface{})
	wait     chan interface{}
	wg       sync.WaitGroup
	execPool chan jobs.Handler
	err      jobs.ErrorHandler
}

// create new tube consumer and producer
func newTube(
	pipe *jobs.Pipeline,
	connPool *cpool.ConnPool,
	reserve time.Duration,
	listener func(event int, ctx interface{}),
) (*tube, error) {
	if pipe.String("tube", "") == "" {
		return nil, errors.New("missing `tube` parameter on beanstalk pipeline")
	}

	return &tube{
		pipe:     pipe,
		tube:     &beanstalk.Tube{Name: pipe.String("tube", "")},
		tubeSet:  beanstalk.NewTubeSet(nil, pipe.String("tube", "")),
		connPool: connPool,
		reserve:  reserve,
		lsn:      listener,
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
	atomic.StoreInt32(&t.active, 1)

	for {
		select {
		case <-t.wait:
			return
		default:
			t.wg.Add(1)
			t.connPool.Exec(t.consume, t.reserve)
			t.wg.Done()
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
}

// put data into pool or return error (no wait)
func (t *tube) put(
	data []byte,
	attempt int,
	delay time.Duration,
	timeout time.Duration,
	cmdTimeout time.Duration,
) (id string, err error) {
	t.wg.Add(1)
	defer t.wg.Done()

	var bid uint64

	// execute operation on first free conn
	err = t.connPool.Exec(func(c interface{}) error {
		t.mut.Lock()
		defer t.mut.Unlock()

		t.tube.Conn = c.(*beanstalk.Conn)

		bid, err = t.tube.Put(data, 0, delay, timeout)
		return wrapErr(err, false)
	}, cmdTimeout)

	return jid(bid), err
}

// return tube stats
func (t *tube) stat(cmdTimeout time.Duration) (stat *jobs.Stat, err error) {
	t.wg.Add(1)
	defer t.wg.Done()

	values := make(map[string]string)

	err = t.connPool.Exec(func(c interface{}) error {
		t.mut.Lock()
		defer t.mut.Unlock()

		t.tube.Conn = c.(*beanstalk.Conn)

		values, err = t.tube.Stats()
		return wrapErr(err, false)
	}, cmdTimeout)

	if err != nil {
		return nil, err
	}

	stat = &jobs.Stat{InternalName: t.tube.Name}

	if v, err := strconv.Atoi(values["current-jobs-ready"]); err == nil {
		stat.Queue = int64(v)
	}

	if v, err := strconv.Atoi(values["current-jobs-reserved"]); err == nil {
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

	if err != nil {
		t.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: t.pipe, Caused: err})
		return nil
	}

	t.wg.Add(1)
	go func(h jobs.Handler, c *beanstalk.Conn) {
		defer t.wg.Done()

		err = h(jid(id), j)
		t.execPool <- h

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

// throw handles service, server and pool events.
func (t *tube) throw(event int, ctx interface{}) {
	t.lsn(event, ctx)
}
