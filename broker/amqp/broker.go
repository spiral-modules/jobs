package amqp

import (
	"errors"
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/spiral/jobs"
	"sync"
	"time"
)

// Broker represents AMQP broker.
type Broker struct {
	cfg         *Config
	lsns        []func(event int, ctx interface{})
	publishPool *chanPool
	consumePool *chanPool
	mu          sync.Mutex
	wait        chan error
	queues      map[*jobs.Pipeline]*queue
}

// AddListener attaches server event watcher.
func (b *Broker) AddListener(l func(event int, ctx interface{})) {
	b.lsns = append(b.lsns, l)
}

// Init configures AMQP job broker (always 2 connections).
func (b *Broker) Init(cfg *Config) (ok bool, err error) {
	b.cfg = cfg
	b.queues = make(map[*jobs.Pipeline]*queue)

	conn, err := newConn(b.cfg.Addr, time.Second)
	if err != nil {
		return false, err
	}
	b.publishPool = conn

	conn, err = newConn(b.cfg.Addr, time.Second)
	if err != nil {
		return false, err
	}
	b.consumePool = conn

	return true, nil
}

// Register broker pipeline.
func (b *Broker) Register(pipe *jobs.Pipeline) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	q, err := newQueue(pipe, b.publishPool, b.consumePool, b.throw)
	if err != nil {
		return err
	}

	b.queues[pipe] = q

	return nil
}

// Serve broker pipelines.
func (b *Broker) Serve() error {
	b.mu.Lock()
	for _, q := range b.queues {
		go q.serve()
	}
	b.wait = make(chan error)
	b.mu.Unlock()

	return <-b.wait
}

// Stop all pipelines.
func (b *Broker) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.wait == nil {
		return
	}

	for _, q := range b.queues {
		q.stop()
	}

	b.wait <- nil
}

// Consume configures pipeline to be consumed. With execPool to nil to disable consuming. Method can be called before
// the service is started!
func (b *Broker) Consume(pipe *jobs.Pipeline, execPool chan jobs.Handler, errHandler jobs.ErrorHandler) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	q, ok := b.queues[pipe]
	if !ok {
		return errors.New("invalid pipeline")
	}

	q.stop()

	if err := q.configure(execPool, errHandler); err != nil {
		return err
	}

	if b.wait != nil && q.execPool != nil {
		go q.serve()
	}

	return nil
}

// Push job into the worker.
func (b *Broker) Push(pipe *jobs.Pipeline, j *jobs.Job) (string, error) {
	if err := b.isServing(); err != nil {
		return "", err
	}

	id, err := uuid.NewV4()
	if err != nil {
		return "", err
	}

	q := b.queue(pipe)
	if q == nil {
		return "", fmt.Errorf("undefined queue `%s`", pipe.Name())
	}

	if err := q.publish(id.String(), j.Body(), 0, j.Options); err != nil {
		return "", err
	}

	return id.String(), nil
}

// Stat must fetch statistics about given pipeline or return error.
func (b *Broker) Stat(pipe *jobs.Pipeline) (stat *jobs.Stat, err error) {
	if err := b.isServing(); err != nil {
		return nil, err
	}

	q := b.queue(pipe)
	if q == nil {
		return nil, fmt.Errorf("undefined queue `%s`", pipe.Name())
	}

	queue, err := q.inspect()
	if err != nil {
		return nil, nil
	}

	// todo: improve approximation
	return &jobs.Stat{
		InternalName: queue.Name,
		Queue:        int64(queue.Messages),
		Active:       int64(queue.Consumers),
	}, nil
}

// queue returns queue associated with the pipeline.
func (b *Broker) queue(pipe *jobs.Pipeline) *queue {
	b.mu.Lock()
	defer b.mu.Unlock()

	q, ok := b.queues[pipe]
	if !ok {
		return nil
	}

	return q
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
