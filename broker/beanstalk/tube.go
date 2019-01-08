package beanstalk

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/beanstalkd/go-beanstalk"
	"github.com/spiral/jobs"
	"log"
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
	reserve time.Duration

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

func (e *entry) ID() string {
	return fmt.Sprintf("%v", e.id)
}

// create new tube consumer and producer
func newTube(
	pipe *jobs.Pipeline,
	sharedConn *conn,
	reserve time.Duration,
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
	atomic.StoreInt32(&t.active, 1)

	conn, err := connector.newConn()
	defer conn.Close()

	if err != nil {
		t.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: t.pipe, Caused: err})
		return
	}

	for e := range t.jobs(conn, prefetch) {
		t.wg.Add(1)
		h := <-t.execPool
		go func() {
			defer t.wg.Done()

			err := t.consume(conn, <-t.execPool, e)
			t.execPool <- h
			log.Println("done")
			if err != nil {
				t.throw(jobs.EventPipelineError, &jobs.PipelineError{
					Pipeline: t.pipe,
					Caused:   err,
				})
			}
		}()
	}

	log.Println("done serving")
	t.wg.Wait()
	log.Println("wait for complete")
}

// fetch jobs
func (t *tube) jobs(cn *conn, prefetch int) chan entry {
	entries := make(chan entry, prefetch)

	go func() {
		for {
			t.muw.Lock()
			select {
			case <-t.wait:
				log.Println("got wait on jobs")
				t.muw.Unlock()
				return
			default:
				conn, err := cn.Acquire()
				if err != nil {
					t.muw.Unlock()
					// appropriate
					continue
				}

				t.tubeSet.Conn = conn

				id, body, err := t.tubeSet.Reserve(t.reserve)
				if err != nil {
					cn.Release(err)
					t.muw.Unlock()

					if isConnError(err) {
						t.throw(jobs.EventPipelineError, &jobs.PipelineError{
							Pipeline: t.pipe,
							Caused:   err,
						})
					}

					continue
				}

				j := &jobs.Job{}
				err = json.Unmarshal(body, j)

				if err != nil {
					cn.Release(err)
					t.muw.Unlock()

					t.throw(jobs.EventPipelineError, &jobs.PipelineError{
						Pipeline: t.pipe,
						Caused:   err,
					})

					continue
				}

				// got the j, it will block eof() until wg is freed
				cn.Release(err)
				t.muw.Unlock() // must drain

				entries <- entry{id: id, job: j}
			}
		}
	}()

	return entries
}

// consume job
func (t *tube) consume(cn *conn, h jobs.Handler, e entry) error {
	err := h(e.ID(), e.job)
	t.execPool <- h

	conn, err := cn.Acquire()
	if err != nil {
		return err
	}

	if err == nil {
		return cn.Release(conn.Delete(e.id))
	}

	stat, statErr := conn.StatsJob(e.id)
	if statErr != nil {
		return cn.Release(statErr)
	}

	t.err(e.ID(), e.job, err)

	reserves, _ := strconv.Atoi(stat["reserves"])
	if reserves != 0 && e.job.Options.CanRetry(reserves) {
		return cn.Release(conn.Release(
			e.id,
			0,
			e.job.Options.RetryDuration(),
		))
	}

	return cn.Release(conn.Bury(e.id, 0))
}

// stop tube consuming
func (t *tube) stop() {
	if atomic.LoadInt32(&t.active) == 0 {
		return
	}

	atomic.StoreInt32(&t.active, 0)

	t.muw.Lock()
	close(t.wait)
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
