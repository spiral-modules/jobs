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
	active int32
	pipe   *jobs.Pipeline

	// beanstalk drivers
	mut     sync.Mutex
	tube    *beanstalk.Tube
	tubeSet *beanstalk.TubeSet

	// socket pool
	connPool *cpool.ConnPool

	// durations
	reserve    time.Duration
	cmdTimeout time.Duration

	// consuming events
	listener func(event int, ctx interface{})

	// stop channel
	wait chan interface{}

	// active operations
	muw sync.RWMutex
	wg  sync.WaitGroup

	// exec handlers
	fetchPool chan interface{}
	execPool  chan jobs.Handler
	err       jobs.ErrorHandler
}

// create new tube consumer and producer
func newTube(
	pipe *jobs.Pipeline,
	connPool *cpool.ConnPool,
	reserve time.Duration,
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
		cmdTimeout: cmdTimeout,
		listener:   listener,
	}, nil
}

// associate tube with new consume pool
func (t *tube) configure(execPool chan jobs.Handler, err jobs.ErrorHandler) error {
	t.execPool = execPool
	t.err = err
	return nil
}

// run consumers
func (t *tube) serve(prefetch int) {
	t.wait = make(chan interface{})
	t.fetchPool = make(chan interface{}, prefetch)
	atomic.StoreInt32(&t.active, 1)

	for i := 0; i < prefetch; i++ {
		t.fetchPool <- nil
	}

	for {
		conn, id, job, stop := t.fetchJob()
		if stop {
			return
		}

		if job == nil {
			continue
		}

		go func() {
			err := t.consume(conn, <-t.execPool, id, job)
			t.connPool.Release(conn, wrapErr(err))
			t.wg.Done()

			if err != nil {
				t.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: t.pipe, Caused: err})
			}
		}()
	}
}

// fetchJob and allocate connection. fetchPool must be refilled manually.
func (t *tube) fetchJob() (conn *beanstalk.Conn, id uint64, job *jobs.Job, stop bool) {
	t.muw.Lock()
	defer t.muw.Unlock()

	select {
	case <-t.wait:
		return nil, 0, nil, true
	case <-t.fetchPool:
		conn, err := t.connPool.Allocate(t.cmdTimeout)
		if err != nil {
			t.fetchPool <- nil
			return nil, 0, nil, false
		}

		t.tubeSet.Conn = conn.(*beanstalk.Conn)

		id, body, err := t.tubeSet.Reserve(t.reserve)
		if err != nil {
			// do not report reserve errors such as timeouts, conn errors will be reported by the connPool
			t.connPool.Release(conn, wrapErr(err))
			t.fetchPool <- nil
			return nil, 0, nil, false
		}

		// got the job!
		t.wg.Add(1)

		job = &jobs.Job{}
		err = json.Unmarshal(body, job)

		if err != nil {
			t.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: t.pipe, Caused: err})
			t.connPool.Release(conn, wrapErr(err))
			t.fetchPool <- nil
			return nil, 0, nil, false
		}

		// fetchPool and connPool will be refilled by consume method
		return conn.(*beanstalk.Conn), id, job, false
	}
}

// consume job
func (t *tube) consume(conn *beanstalk.Conn, h jobs.Handler, id uint64, j *jobs.Job) error {
	err := h(jid(id), j)
	t.execPool <- h
	t.fetchPool <- nil

	if err == nil {
		return conn.Delete(id)
	}

	stat, statErr := conn.StatsJob(id)
	if statErr != nil {
		return statErr
	}

	reserves, _ := strconv.Atoi(stat["reserves"])
	if j.Options.CanRetry(reserves) {
		return conn.Release(id, 0, j.Options.RetryDuration())
	}

	t.err(jid(id), j, err)
	return conn.Bury(id, 0)
}

// stop tube consuming
func (t *tube) stop() {
	if atomic.LoadInt32(&t.active) == 0 {
		return
	}

	atomic.StoreInt32(&t.active, 0)

	close(t.wait)
	t.muw.Lock()
	t.wg.Wait()
	t.muw.Unlock()
}

// put data into pool or return error (no wait)
func (t *tube) put(data []byte, attempt int, delay, rrt time.Duration) (id string, err error) {
	var bid uint64
	conn, err := t.connPool.Allocate(t.cmdTimeout)
	if err != nil {
		return "", err
	}

	t.mut.Lock()
	t.tube.Conn = conn.(*beanstalk.Conn)
	bid, err = t.tube.Put(data, 0, delay, rrt)
	t.mut.Unlock()

	t.connPool.Release(conn, wrapErr(err))

	return jid(bid), err
}

// return tube stats
func (t *tube) stat() (stat *jobs.Stat, err error) {
	conn, err := t.connPool.Allocate(t.cmdTimeout)
	if err != nil {
		return nil, err
	}

	t.mut.Lock()
	t.tube.Conn = conn.(*beanstalk.Conn)
	values, err := t.tube.Stats()
	t.mut.Unlock()

	t.connPool.Release(conn, wrapErr(err))

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

// throw handles service, server and pool events.
func (t *tube) throw(event int, ctx interface{}) {
	t.listener(event, ctx)
}

// jid converts job id into string.
func jid(id uint64) string {
	if id == 0 {
		return ""
	}
	return strconv.FormatUint(id, 10)
}

// wrapError into conn error when detected. softErr would not wrap any of no connection errors.
func wrapErr(err error) error {
	if err == nil {
		return nil
	}

	if cpool.IsConnError(err) {
		return cpool.ConnError{Err: err}
	}

	return err
}
