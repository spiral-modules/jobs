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
	mu       sync.Mutex
	wait     chan interface{}
	connPool *cpool.ConnPool
	tubes    map[*jobs.Pipeline]*tube
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
		t, err := newTube(p, b.connPool)
		if err != nil {
			return err
		}

		b.tubes[p] = t
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

	for _, t := range b.tubes {
		go t.serve()
	}

	b.wait = make(chan interface{})
	b.mu.Unlock()

	<-b.wait

	return nil
}

// wait local broker.
func (b *Broker) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.wait == nil {
		return
	}

	for _, t := range b.tubes {
		t.stop()
	}

	close(b.wait)
	b.wait = nil

	b.connPool.Destroy()
}

// Consuming enables or disabled pipeline consuming.
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

	if b.wait != nil {
		// resume consuming
		go t.serve()
	}

	return nil
}

// Push job into the worker.
func (b *Broker) Push(pipe *jobs.Pipeline, j *jobs.Job) (string, error) {
	b.mu.Lock()
	if b.wait == nil {
		b.mu.Unlock()
		return "", errors.New("broker is not running")
	}
	b.mu.Unlock()

	t := b.Tube(pipe)
	if t == nil {
		return "", fmt.Errorf("undefined tube `%s`", pipe.Name())
	}

	data, err := json.Marshal(j)
	if err != nil {
		return "", err
	}

	return t.put(data, 0, j.Options.DelayDuration(), j.Options.RetryDuration())
}

// Stat must fetch statistics about given pipeline or return error.
func (b *Broker) Stat(pipe *jobs.Pipeline) (stat *jobs.Stat, err error) {
	t := b.Tube(pipe)
	if t == nil {
		return nil, fmt.Errorf("undefined tube `%s`", pipe.Name())
	}

	return t.stat()
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
