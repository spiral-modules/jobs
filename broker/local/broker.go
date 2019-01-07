package local

import (
	"fmt"
	"github.com/gofrs/uuid"
	"github.com/pkg/errors"
	"github.com/spiral/jobs"
	"sync"
)

// Broker run queue using local goroutines.
type Broker struct {
	mu     sync.Mutex
	wait   chan interface{}
	queues map[*jobs.Pipeline]*queue
}

// Init configures broker.
func (b *Broker) Init() (bool, error) {
	return true, nil
}

// Register broker specific pipelines.
func (b *Broker) Register(pipes []*jobs.Pipeline) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.queues = make(map[*jobs.Pipeline]*queue)
	for _, p := range pipes {
		b.queues[p] = newQueue()
	}

	return nil
}

// Serve broker pipelines.
func (b *Broker) Serve() error {
	// start consuming
	b.mu.Lock()
	for _, q := range b.queues {
		go q.serve()
	}
	b.wait = make(chan interface{})
	b.mu.Unlock()

	<-b.wait

	return nil
}

// Stop all pipelines.
func (b *Broker) Stop() {
	if b.wait == nil {
		return
	}

	// wait consuming after all jobs are complete
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, q := range b.queues {
		// wait for stop
		q.stop()
	}

	close(b.wait)
}

// Consume configures pipeline to be consumed. Set execPool to nil to disable consuming. Method can be called before
// the service is started!
func (b *Broker) Consume(pipe *jobs.Pipeline, execPool chan jobs.Handler, errHandler jobs.ErrorHandler) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	q, ok := b.queues[pipe]
	if !ok {
		return fmt.Errorf("undefined queue `%s`", pipe.Name())
	}

	q.stop()

	if err := q.configure(execPool, errHandler); err != nil {
		return err
	}

	if b.wait != nil && q.execPool != nil {
		// resume consuming
		go q.serve()
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

	q := b.queue(pipe)
	if q == nil {
		return "", fmt.Errorf("undefined queue `%s`", pipe.Name())
	}

	id, err := uuid.NewV4()
	if err != nil {
		return "", err
	}

	go q.push(id.String(), j, 0, j.Options.DelayDuration())

	return id.String(), nil
}

// Stat must fetch statistics about given pipeline or return error.
func (b *Broker) Stat(pipe *jobs.Pipeline) (stat *jobs.Stat, err error) {
	q := b.queue(pipe)
	if q == nil {
		return nil, fmt.Errorf("undefined queue `%s`", pipe.Name())
	}

	return q.stat, nil
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
