package local

import (
	"github.com/pkg/errors"
	"github.com/satori/go.uuid"
	"github.com/spiral/jobs"
	"sync"
	"sync/atomic"
)

// Broker run queue using local goroutines.
type Broker struct {
	running int32
	mu      sync.Mutex
	stop    chan interface{}
	queues  map[*jobs.Pipeline]*queue
}

// Init configures local job broker.
func (b *Broker) Init() (bool, error) {
	return true, nil
}

// serve local broker.
func (b *Broker) Serve() error {
	// enable consuming?
	atomic.StoreInt32(&b.running, 1)

	// start consuming
	b.mu.Lock()
	for _, q := range b.queues {
		go q.serve()
	}
	b.stop = make(chan interface{})
	b.mu.Unlock()

	<-b.stop

	return nil
}

// stop local broker.
func (b *Broker) Stop() {
	if atomic.LoadInt32(&b.running) == 0 {
		return
	}

	// stop consuming after all jobs are complete
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, q := range b.queues {
		q.stop()
	}

	close(b.stop)
}

// Listen configures broker with list of pipelines to listen and handler function.
func (b *Broker) Register(pipes []*jobs.Pipeline) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.queues = make(map[*jobs.Pipeline]*queue)
	for _, p := range pipes {
		b.queues[p] = newQueue()
	}

	return nil
}

// Consume enables or disabled pipeline consuming.
func (b *Broker) Consume(pipes []*jobs.Pipeline, execPool chan jobs.Handler, err jobs.ErrorHandler) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	// WTF

	for _, pipe := range pipes {
		q, ok := b.queues[pipe]
		if !ok {
			return errors.New("invalid pipeline")
		}

		q.stop()

		if err := q.configure(execPool, err); err != nil {
			return err
		}

		if atomic.LoadInt32(&b.running) == 1 {
			go q.serve()
		}
	}

	return nil
}

// Push job into the worker.
func (b *Broker) Push(pipe *jobs.Pipeline, j *jobs.Job) (string, error) {
	if atomic.LoadInt32(&b.running) != 1 {
		return "", errors.New("broker is not running")
	}

	b.mu.Lock()
	q, ok := b.queues[pipe]
	b.mu.Unlock()

	if !ok {
		return "", errors.New("invalid pipeline")
	}

	id, err := uuid.NewV4()
	if err != nil {
		return "", err
	}

	go q.push(id.String(), j, 0, j.Options.DelayDuration())

	return id.String(), nil
}

// Stat must fetch statistics about given pipeline or return error.
func (b *Broker) Stat(p *jobs.Pipeline) (stat *jobs.Stat, err error) {
	return nil, nil
}
