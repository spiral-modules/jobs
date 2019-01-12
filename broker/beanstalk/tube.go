package beanstalk

import (
	"encoding/json"
	"fmt"
	"github.com/beanstalkd/go-beanstalk"
	"github.com/spiral/jobs"
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
	reserve    time.Duration
	sharedConn *conn

	// tube events
	lsn func(event int, ctx interface{})

	// stop channel
	wait chan interface{}

	// active operations
	muw sync.RWMutex
	wg  sync.WaitGroup

	// exec handlers
	execPool chan jobs.Handler
	err      jobs.ErrorHandler
}

type entry struct {
	id  uint64
	job *jobs.Job
}

func (e *entry) String() string {
	return fmt.Sprintf("%v", e.id)
}

// create new tube consumer and producer
func newTube(pipe *jobs.Pipeline, lsn func(event int, ctx interface{})) (*tube, error) {
	if pipe.String("tube", "") == "" {
		return nil, fmt.Errorf("missing `tube` parameter on beanstalk pipeline")
	}

	return &tube{
		pipe:    pipe,
		tube:    &beanstalk.Tube{Name: pipe.String("tube", "")},
		tubeSet: beanstalk.NewTubeSet(nil, pipe.String("tube", "")),
		reserve: pipe.Duration("reserve", time.Second),
		lsn:     lsn,
	}, nil
}

// associate tube with new do pool
func (t *tube) configure(execPool chan jobs.Handler, err jobs.ErrorHandler) error {
	t.execPool = execPool
	t.err = err

	return nil
}

// run consumers
func (t *tube) serve(sharedConn *conn, connector connFactory) {
	t.wait = make(chan interface{})
	atomic.StoreInt32(&t.active, 1)

	// connection to publish messages
	t.sharedConn = sharedConn

	// tube specific consume connection
	conn, err := connector.newConn()
	defer conn.Close()

	if err != nil {
		t.report(err)
		return
	}

	recv := t.consume(conn)
	for {
		select {
		case e, ok := <-recv:
			if !ok {
				// closed
				return
			}

			h := <-t.execPool
			go func(h jobs.Handler) {
				err := t.do(conn, <-t.execPool, e)
				t.execPool <- h
				t.wg.Done()

				if err != nil {
					t.report(err)
				}
			}(h)
		}
	}
}

// fetch consume
func (t *tube) consume(cn *conn) chan entry {
	entries := make(chan entry)

	go func() {
		for {
			t.muw.Lock()
			select {
			case <-t.wait:
				t.muw.Unlock()
				close(entries)
				return
			default:
				conn, err := cn.acquire()
				if err != nil {
					t.muw.Unlock()

					// connection is dead wait till restore
					continue
				}

				t.tubeSet.Conn = conn

				id, body, err := t.tubeSet.Reserve(t.reserve)
				if err != nil {
					cn.release(err)
					t.muw.Unlock()

					if isConnError(err) {
						t.report(err)
					}

					continue
				}

				j := &jobs.Job{}
				err = json.Unmarshal(body, j)

				if err != nil {
					cn.release(err)
					t.muw.Unlock()
					t.report(err)

					continue
				}

				// got the job, it will block eof() until wg is freed
				cn.release(err)
				t.wg.Add(1)
				entries <- entry{id: id, job: j}
				t.muw.Unlock() // must drain
			}
		}
	}()

	return entries
}

// do job
func (t *tube) do(cn *conn, h jobs.Handler, e entry) error {
	err := h(e.String(), e.job)
	t.execPool <- h

	conn, err := cn.acquire()
	if err != nil {
		return err
	}

	if err == nil {
		return cn.release(conn.Delete(e.id))
	}

	stat, statErr := conn.StatsJob(e.id)
	if statErr != nil {
		return cn.release(statErr)
	}

	t.err(e.String(), e.job, err)

	reserves, ok := strconv.Atoi(stat["reserves"])
	if ok != nil || !e.job.Options.CanRetry(reserves) {
		return cn.release(conn.Bury(e.id, 0))
	}

	return cn.release(conn.Release(e.id, 0, e.job.Options.RetryDuration()))
}

// stop tube consuming
func (t *tube) stop() {
	if atomic.LoadInt32(&t.active) == 0 {
		return
	}

	atomic.StoreInt32(&t.active, 0)

	t.muw.Lock()
	close(t.wait)
	t.wg.Wait()
	t.muw.Unlock()
}

// put data into pool or return error (no wait)
func (t *tube) put(data []byte, attempt int, delay, rrt time.Duration) (id string, err error) {
	conn, err := t.sharedConn.acquire()
	if err != nil {
		return "", err
	}

	var bid uint64

	t.mut.Lock()
	t.tube.Conn = conn
	bid, err = t.tube.Put(data, 0, delay, rrt)
	t.mut.Unlock()

	t.sharedConn.release(err)

	return strconv.FormatUint(bid, 10), err
}

// return tube stats
func (t *tube) stat() (stat *jobs.Stat, err error) {
	conn, err := t.sharedConn.acquire()
	if err != nil {
		return nil, err
	}

	t.mut.Lock()
	t.tube.Conn = conn
	values, err := t.tube.Stats()
	t.mut.Unlock()

	t.sharedConn.release(err)

	stat = &jobs.Stat{InternalName: t.tube.Name}

	if v, err := strconv.Atoi(values["current-consume-ready"]); err == nil {
		stat.Queue = int64(v)
	}

	if v, err := strconv.Atoi(values["current-consume-reserved"]); err == nil {
		stat.Active = int64(v)
	}

	if v, err := strconv.Atoi(values["current-consume-delayed"]); err == nil {
		stat.Delayed = int64(v)
	}

	return stat, err
}

// report tube specific error
func (t *tube) report(err error) {
	t.lsn(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: t.pipe, Caused: err})
}
