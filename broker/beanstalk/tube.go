package beanstalk

import (
	"encoding/json"
	"errors"
	"github.com/beanstalkd/go-beanstalk"
	"github.com/spiral/jobs"
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

	// only for put and stat operations
	sharedConn *conn

	// durations
	reserve    time.Duration
	cmdTimeout time.Duration

	// tube events
	lsn func(event int, ctx interface{})

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
	sharedConn *conn,
	reserve time.Duration,
	cmdTimeout time.Duration,
	lsn func(event int, ctx interface{}),
) (*tube, error) {
	if pipe.String("tube", "") == "" {
		return nil, errors.New("missing `tube` parameter on beanstalk pipeline")
	}

	return &tube{
		pipe:       pipe,
		tube:       &beanstalk.Tube{Name: pipe.String("tube", "")},
		tubeSet:    beanstalk.NewTubeSet(nil, pipe.String("tube", "")),
		sharedConn: sharedConn,
		reserve:    reserve,
		cmdTimeout: cmdTimeout,
		lsn:        lsn,
	}, nil
}

// associate tube with new consume pool
func (t *tube) configure(execPool chan jobs.Handler, err jobs.ErrorHandler) error {
	t.execPool = execPool
	t.err = err

	return nil
}

// run consumers
func (t *tube) serve(connector connector, prefetch int) {
	t.wait = make(chan interface{})
	t.fetchPool = make(chan interface{}, prefetch)
	atomic.StoreInt32(&t.active, 1)

	consumeConn, err := connector.newConn()
	defer consumeConn.Close()

	if err != nil {
		t.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: t.pipe, Caused: err})
		return
	}

	for i := 0; i < prefetch; i++ {
		t.fetchPool <- nil
	}

	for {
		id, job, eof := t.fetchJob(consumeConn)

		if eof {
			return
		}

		if job == nil {
			continue
		}

		go func() {
			err := t.consume(consumeConn, <-t.execPool, id, job)
			t.fetchPool <- nil
			t.wg.Done()

			if err != nil {
				t.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: t.pipe, Caused: err})
			}
		}()
	}
}

// fetchJob and allocate connection. fetchPool must be refilled manually.
func (t *tube) fetchJob(consumeConn *conn) (id uint64, job *jobs.Job, eof bool) {
	t.muw.Lock()
	defer t.muw.Unlock()

	select {
	case <-t.wait:
		return 0, nil, true
	case <-t.fetchPool:
		conn, err := consumeConn.Acquire()
		if err != nil {
			t.fetchPool <- nil
			return 0, nil, false
		}

		t.tubeSet.Conn = conn

		id, body, err := t.tubeSet.Reserve(t.reserve)
		if err != nil {
			consumeConn.Release(err)
			t.fetchPool <- nil
			return 0, nil, false
		}

		job = &jobs.Job{}
		err = json.Unmarshal(body, job)

		if err != nil {
			t.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: t.pipe, Caused: err})
			consumeConn.Release(err)
			t.fetchPool <- nil
			return 0, nil, false
		}

		// got the job, it will block eof() until wg is freed
		t.wg.Add(1)
		consumeConn.Release(err)

		// fetchPool and connPool will be refilled by consume method
		return id, job, false
	}
}

// consume job
func (t *tube) consume(consumeConn *conn, h jobs.Handler, id uint64, j *jobs.Job) error {
	err := h(jid(id), j)
	t.execPool <- h

	conn, err := consumeConn.Acquire()
	if err != nil {
		return err
	}

	if err == nil {
		return consumeConn.Release(conn.Delete(id))
	}

	stat, statErr := conn.StatsJob(id)
	if statErr != nil {
		return consumeConn.Release(statErr)
	}

	t.err(jid(id), j, err)

	reserves, _ := strconv.Atoi(stat["reserves"])
	if reserves != 0 && j.Options.CanRetry(reserves) {
		return consumeConn.Release(conn.Release(id, 0, j.Options.RetryDuration()))
	}

	return consumeConn.Release(conn.Bury(id, 0))
}

// stop tube consuming
func (t *tube) stop() {
	if atomic.LoadInt32(&t.active) == 0 {
		return
	}

	atomic.StoreInt32(&t.active, 0)

	close(t.wait)
	t.muw.Lock()
	t.wg.Wait() // wait for all the jobs to complete
	t.muw.Unlock()
}

// put data into pool or return error (no wait)
func (t *tube) put(data []byte, attempt int, delay, rrt time.Duration) (id string, err error) {
	conn, err := t.sharedConn.Acquire()
	if err != nil {
		return "", err
	}

	var bid uint64

	t.mut.Lock()
	t.tube.Conn = conn
	bid, err = t.tube.Put(data, 0, delay, rrt)
	t.mut.Unlock()

	t.sharedConn.Release(err)

	return jid(bid), err
}

// return tube stats
func (t *tube) stat() (stat *jobs.Stat, err error) {
	conn, err := t.sharedConn.Acquire()
	if err != nil {
		return nil, err
	}

	t.mut.Lock()
	t.tube.Conn = conn
	values, err := t.tube.Stats()
	t.mut.Unlock()

	t.sharedConn.Release(err)

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
	t.lsn(event, ctx)
}

// jid converts job id into string.
func jid(id uint64) string {
	if id == 0 {
		return ""
	}
	return strconv.FormatUint(id, 10)
}
