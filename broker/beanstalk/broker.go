package beanstalk

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/spiral/jobs"
	"sync"
)

// Broker run jobs using Broker service.
type Broker struct {
	cfg        *Config
	lsns       []func(event int, ctx interface{})
	mu         sync.Mutex
	wait       chan error
	sharedConn *conn
	tubes      map[*jobs.Pipeline]*tube
}

// AddListener attaches server event watcher.
func (b *Broker) AddListener(l func(event int, ctx interface{})) {
	b.lsns = append(b.lsns, l)
}

// Init configures broker.
func (b *Broker) Init(cfg *Config) (bool, error) {
	b.cfg = cfg
	b.tubes = make(map[*jobs.Pipeline]*tube)

	conn, err := b.cfg.newConn()
	if err != nil {
		return false, err
	}
	b.sharedConn = conn

	return true, nil
}

// Register broker pipeline.
func (b *Broker) Register(pipe *jobs.Pipeline) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	t, err := newTube(
		pipe,
		b.sharedConn,            // available connections
		b.cfg,                   // connector
		b.cfg.ReserveDuration(), // for how long tube should be wait for job to come
		b.cfg.TimeoutDuration(), // how much time is given to allocate connection
		b.throw,                 // event lsn
	)

	if err != nil {
		return err
	}

	b.tubes[pipe] = t

	return nil
}

// Serve broker pipelines.
func (b *Broker) Serve() (err error) {
	b.mu.Lock()
	for _, t := range b.tubes {
		if t.execPool != nil {
			go t.serve(b.cfg.Prefetch)
		}
	}

	b.wait = make(chan error)
	b.mu.Unlock()

	return <-b.wait
}

// Stop all pipelines.
func (b *Broker) Stop() {
	b.stopError(nil)
}

// Consume configures pipeline to be consumed. Set execPool to nil to disable consuming. Method can be called before
// the service is started!
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
		go t.serve(b.cfg.Prefetch)
	}

	return nil
}

// Push job into the worker.
func (b *Broker) Push(pipe *jobs.Pipeline, j *jobs.Job) (string, error) {
	if err := b.isServing(); err != nil {
		return "", err
	}

	t := b.tube(pipe)
	if t == nil {
		return "", fmt.Errorf("undefined tube `%s`", pipe.Name())
	}

	data, err := json.Marshal(j)
	if err != nil {
		return "", err
	}

	return t.put(data, 0, j.Options.DelayDuration(), j.Options.TimeoutDuration())
}

// Stat must fetch statistics about given pipeline or return error.
func (b *Broker) Stat(pipe *jobs.Pipeline) (stat *jobs.Stat, err error) {
	if err := b.isServing(); err != nil {
		return nil, err
	}

	t := b.tube(pipe)
	if t == nil {
		return nil, fmt.Errorf("undefined tube `%s`", pipe.Name())
	}

	return t.stat()
}

// queue returns queue associated with the pipeline.
func (b *Broker) tube(pipe *jobs.Pipeline) *tube {
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

	b.wait <- nil
	b.sharedConn.Close()
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
