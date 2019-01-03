package beanstalk

import (
	"encoding/json"
	"github.com/beanstalkd/go-beanstalk"
	"github.com/spiral/jobs"
	"github.com/spiral/jobs/cpool"
	"strconv"
	"sync"
	"sync/atomic"
)

// Broker run jobs using Broker service.
type Broker struct {
	status   int32
	cfg      *Config
	connPool *cpool.ConnPool
	execPool chan jobs.Handler
	err      jobs.ErrorHandler
	mu       sync.Mutex
	stop     chan interface{}
	tubes    map[*jobs.Pipeline]*Tube
	tubeSet  *beanstalk.TubeSet
}

// Status returns broken status.
func (b *Broker) Status() jobs.BrokerStatus {
	return jobs.BrokerStatus(atomic.LoadInt32(&b.status))
}

// Listen configures broker with list of tubes to listen and handler function. Local broker groups all tubes
// together.
func (b *Broker) Listen(pipelines []*jobs.Pipeline, execPool chan jobs.Handler, err jobs.ErrorHandler) error {
	b.tubes = make(map[*jobs.Pipeline]*Tube)
	for _, p := range pipelines {
		if err := b.registerTube(p); err != nil {
			return err
		}
	}

	b.execPool = execPool
	b.err = err
	return nil
}

// Init configures local job broker.
func (b *Broker) Init(cfg *Config) (bool, error) {
	b.cfg = cfg
	b.connPool = &cpool.ConnPool{
		NumConn: cfg.Connections,
		Open:    func() (c interface{}, e error) { return b.cfg.Conn() },
		Close:   func(c interface{}) { c.(*beanstalk.Conn).Close() },
	}

	return true, nil
}

// serve tubes.
func (b *Broker) Serve() (err error) {
	if err = b.connPool.Init(); err != nil {
		return err
	}

	b.mu.Lock()
	b.stop = make(chan interface{})
	b.mu.Unlock()

	var listen []string
	for _, t := range b.tubes {
		if t.Listen {
			listen = append(listen, t.Name)
		}
	}

	// ready to accept jobs
	atomic.StoreInt32(&b.status, int32(jobs.StatusReady))

	if len(listen) != 0 {
		// conn will be provided later
		b.tubeSet = beanstalk.NewTubeSet(nil, listen...)

		for {
			select {
			case <-b.stop:
				return
			default:
				err = b.connPool.Exec(b.consume)
				if err != nil {
					return
				}
			}
		}
	} else {
		<-b.stop
	}

	return err
}

// stop serving.
func (b *Broker) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()

	atomic.StoreInt32(&b.status, int32(jobs.StatusRegistered))

	if b.stop != nil {
		close(b.stop)
	}

	b.connPool.Destroy()
}

// Push new job to queue
func (b *Broker) Push(p *jobs.Pipeline, j *jobs.Job) (string, error) {
	data, err := json.Marshal(j)
	if err != nil {
		return "", err
	}

	var id uint64

	// execute operation on first free conn
	err = b.connPool.Exec(func(c interface{}) error {
		t := b.tubes[p]

		t.mu.Lock()
		defer t.mu.Unlock()
		t.Conn = c.(*beanstalk.Conn)

		id, err = t.Put(data, 0, j.Options.DelayDuration(), j.Options.TimeoutDuration())
		return pushErr(err, false)
	})

	return jid(id), err
}

// Stat must fetch statistics about given pipeline or return error.
func (b *Broker) Stat(p *jobs.Pipeline) (stat *jobs.Stat, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	err = b.connPool.Exec(func(c interface{}) error {
		t := b.tubes[p]

		t.mu.Lock()
		defer t.mu.Unlock()
		t.Conn = c.(*beanstalk.Conn)

		stat, err = t.fetchStats()
		return pushErr(err, false)
	})

	return
}

// registerTube new beanstalk pipeline
func (b *Broker) registerTube(pipeline *jobs.Pipeline) error {
	tube, err := NewTube(pipeline)
	if err != nil {
		return err
	}

	b.tubes[pipeline] = tube
	return nil
}

// consume job from the server
func (b *Broker) consume(c interface{}) error {
	b.tubeSet.Conn = c.(*beanstalk.Conn)
	id, body, err := b.tubeSet.Reserve(b.cfg.ReserveDuration())

	if err != nil {
		// timeout or other soft error
		return pushErr(err, true)
	}

	var j *jobs.Job
	err = json.Unmarshal(body, &j)
	if err != nil {
		// unable to unmarshal
		return nil
	}

	// todo: add touch
	// todo: add wg

	go func(h jobs.Handler, c *beanstalk.Conn) {
		err = h(jid(id), j)
		b.execPool <- h

		if err == nil {
			c.Delete(id)
			return
		}

		// number of reserves
		stat, _ := c.StatsJob(id)
		reserves, _ := strconv.Atoi(stat["reserves"])

		if j.CanRetry(reserves) {
			// retrying
			c.Release(id, 0, j.Options.RetryDuration())
			return
		}

		b.err(jid(id), j, err)
		c.Bury(id, 0)
	}(<-b.execPool, b.tubeSet.Conn)

	return nil
}

// jid converts job id into string.
func jid(id uint64) string {
	if id == 0 {
		return ""
	}
	return strconv.FormatUint(id, 10)
}

// wrapError into conn error when detected. softErr would not wrap any of no connection errors.
func pushErr(err error, hide bool) error {
	if err == nil {
		return nil
	}

	// yeaaah...
	if cpool.IsPipeError(err) {
		return cpool.PipeErr{Err: err}
	}

	if hide {
		return nil
	}

	return err
}
