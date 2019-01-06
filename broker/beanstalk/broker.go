package beanstalk

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/spiral/jobs"
	"github.com/spiral/jobs/cpool"
	"sync"
)

// Broker run jobs using Broker service.
type Broker struct {
	cfg      *Config
	lsns     []func(event int, ctx interface{})
	mu       sync.Mutex
	wait     chan error
	connPool *cpool.ConnPool
	tubes    map[*jobs.Pipeline]*tube
}

// AddListener attaches server event watcher.
func (b *Broker) AddListener(l func(event int, ctx interface{})) {
	b.lsns = append(b.lsns, l)
}

// Start configures local job broker.
func (b *Broker) Init(cfg *Config) (bool, error) {
	b.cfg = cfg
	b.connPool = cfg.ConnPool()

	return true, nil
}

// Listen configures broker with list of pipelines to listen and handler function.
func (b *Broker) Register(pipes []*jobs.Pipeline) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.tubes = make(map[*jobs.Pipeline]*tube)
	for _, p := range pipes {
		t, err := newTube(p, b.connPool, b.cfg.ReserveDuration(), b.throw)
		if err != nil {
			return err
		}

		b.tubes[p] = t
	}

	if len(b.tubes)+1 > b.connPool.Size {
		// Since each of the tube is listening it's own connection is it possible to run out of connections
		// for pushing jobs in. Low number of connection won't break the server but would affect the performance.
		b.connPool.Size = len(b.tubes) + 1
	}

	return nil
}

// serve local broker.
func (b *Broker) Serve() (err error) {
	b.mu.Lock()

	if err = b.connPool.Start(); err != nil {
		b.mu.Unlock()
		return err
	}
	defer b.connPool.Destroy()

	for _, t := range b.tubes {
		go t.serve()
	}

	b.wait = make(chan error)
	b.mu.Unlock()

	select {
	case err := <-b.wait:
		return err
		// todo: watch pool
	}
}

// wait local broker.
func (b *Broker) Stop() {
	b.stopError(nil)
}

// Consuming enables or disabled pipeline wg.
func (b *Broker) Consume(pipe *jobs.Pipeline, execPool chan jobs.Handler, errHandler jobs.ErrorHandler) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	t, ok := b.tubes[pipe]
	if !ok {
		return errors.New("invalid pipeline")
	}

	t.stop()

	if err := t.configure(execPool, errHandler); err != nil {
		return err
	}

	if b.wait != nil && t.execPool != nil {
		// resume wg
		go t.serve()
	}

	return nil
}

// Push job into the worker.
func (b *Broker) Push(pipe *jobs.Pipeline, j *jobs.Job) (string, error) {
	if err := b.isServing(); err != nil {
		return "", err
	}

	t := b.Tube(pipe)
	if t == nil {
		return "", fmt.Errorf("undefined tube `%s`", pipe.Name())
	}

	data, err := json.Marshal(j)
	if err != nil {
		return "", err
	}

	return t.put(
		data,
		0,
		j.Options.DelayDuration(),
		j.Options.TimeoutDuration(),
		b.cfg.TimeoutDuration(),
	)
}

// Stat must fetch statistics about given pipeline or return error.
func (b *Broker) Stat(pipe *jobs.Pipeline) (stat *jobs.Stat, err error) {
	if err := b.isServing(); err != nil {
		return nil, err
	}

	t := b.Tube(pipe)
	if t == nil {
		return nil, fmt.Errorf("undefined tube `%s`", pipe.Name())
	}

	return t.stat(b.cfg.TimeoutDuration())
}

// Queue returns queue associated with the pipeline.
func (b *Broker) Tube(pipe *jobs.Pipeline) *tube {
	b.mu.Lock()
	defer b.mu.Unlock()

	t, ok := b.tubes[pipe]
	if !ok {
		return nil
	}

	return t
}

// stop broker and send error
func (b *Broker) stopError(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.wait == nil {
		return
	}

	for _, t := range b.tubes {
		t.stop()
	}
	b.wait <- err
}

// check if broker is serving
func (b *Broker) isServing() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.wait == nil {
		return errors.New("broker is not running")
	}

	return nil
}

// throw handles service, server and pool events.
func (b *Broker) throw(event int, ctx interface{}) {
	for _, l := range b.lsns {
		l(event, ctx)
	}
}
