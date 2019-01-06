package beanstalk

import (
	"encoding/json"
	"errors"
	"github.com/beanstalkd/go-beanstalk"
	"github.com/spiral/jobs"
	"github.com/spiral/jobs/cpool"
	"log"
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
	cmdTimeout time.Duration
	lsn        func(event int, ctx interface{})
	wait       chan interface{}
	waitTouch  chan interface{}
	wg         sync.WaitGroup
	execPool   chan jobs.Handler
	err        jobs.ErrorHandler
	mur        sync.Mutex
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
		lsn:        listener,
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

	for {
		select {
		case <-t.wait:
			return
		default:
			conn, err := t.connPool.Allocate(t.cmdTimeout)
			if err != nil {
				// keep trying
				continue
			}

			t.tubeSet.Conn = conn.(*beanstalk.Conn)

			id, body, err := t.tubeSet.Reserve(t.reserve)
			if err != nil {
				// do not report reserve errors such as timeouts, conn errors will be reported by the connPool
				t.connPool.Release(conn, wrapErr(err))
				continue
			}

			var j *jobs.Job
			err = json.Unmarshal(body, &j)
			if err != nil {
				t.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: t.pipe, Caused: err})
				t.connPool.Release(conn, wrapErr(err))
				continue
			}

			t.wg.Add(1)

			go t.consume(conn.(*beanstalk.Conn), id, j)
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
	log.Println("waiting", t.wg)

	t.wg.Wait()

	log.Println("waiting done")
	close(t.waitTouch)
}

// put data into pool or return error (no wait)
func (t *tube) put(data []byte, attempt int, delay, rrt time.Duration) (id string, err error) {
	t.wg.Add(1)
	defer t.wg.Done()

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
	t.wg.Add(1)
	defer t.wg.Done()

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

	if v, err := strconv.Atoi(values["current-jobs-touch"]); err == nil {
		stat.Active = int64(v)
	}

	if v, err := strconv.Atoi(values["current-jobs-delayed"]); err == nil {
		stat.Delayed = int64(v)
	}

	return stat, err
}

// consume job
func (t *tube) consume(conn *beanstalk.Conn, id uint64, j *jobs.Job) {
	defer t.wg.Done()

	// connection leaks here (!!!)
	h := <-t.execPool
	err := h(jid(id), j)
	t.execPool <- h

	if err == nil {
		err = conn.Delete(id)
		if err != nil {
			t.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: t.pipe, Caused: err})
		}

		t.connPool.Release(conn, wrapErr(err))
		return
	}

	stat, err := conn.StatsJob(id)
	if err != nil {
		t.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: t.pipe, Caused: err})

		t.connPool.Release(conn, wrapErr(err))
		return
	}

	reserves, _ := strconv.Atoi(stat["reserves"])

	if j.Options.CanRetry(reserves) {
		err = conn.Release(id, 0, j.Options.RetryDuration())
		if err != nil {
			t.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: t.pipe, Caused: err})
		}

		t.connPool.Release(conn, wrapErr(err))
		return
	}

	t.err(jid(id), j, err)
	err = conn.Bury(id, 0)
	if err != nil {
		t.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: t.pipe, Caused: err})
	}

	t.connPool.Release(conn, wrapErr(err))
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
func wrapErr(err error) error {
	if err == nil {
		return nil
	}

	if cpool.IsConnError(err) {
		return cpool.ConnError{Err: err}
	}

	return err
}
