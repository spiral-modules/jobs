package broker

import (
	"github.com/spiral/jobs"
	"sync"
	"github.com/beanstalkd/go-beanstalk"
	"errors"
	"time"
	"encoding/json"
	"fmt"
)

// Beanstalk run jobs using Beanstalk service.
type Beanstalk struct {
	cfg     *BeanstalkConfig
	mu      sync.Mutex
	stop    chan interface{}
	conn    *beanstalk.Conn
	threads int
	reserve time.Duration
	wg      sync.WaitGroup
	exec    jobs.Handler
	fail    jobs.ErrorHandler
}

// Init configures local job broker.
func (b *Beanstalk) Init(cfg *BeanstalkConfig) (bool, error) {
	if !cfg.Enable {
		return false, nil
	}

	b.cfg = cfg
	return true, nil
}

// Listen configures broker with list of pipelines to listen and handler function. Local broker groups all pipelines
// together.
func (b *Beanstalk) Handle(pipelines []*jobs.Pipeline, h jobs.Handler, f jobs.ErrorHandler) error {
	switch {
	case len(pipelines) < 1:
		// no pipelines to handleThread
		return nil

	case len(pipelines) == 1:
		b.reserve = pipelines[0].Options.Duration("reserve", time.Second)
		b.threads = pipelines[0].Options.Integer("threads", 1)
		if b.threads < 1 {
			return errors.New("beanstalk queue `thread` number must be 1 or higher")
		}

	default:
		return errors.New("beanstalk queue handler expects exactly one pipeline")
	}

	b.exec = h
	b.fail = f
	return nil
}

// Serve local broker.
func (b *Beanstalk) Serve() error {
	conn, err := b.cfg.Conn()
	if err != nil {
		return err
	}
	defer conn.Close()

	b.conn = conn

	b.mu.Lock()
	b.stop = make(chan interface{})
	b.mu.Unlock()

	for i := 0; i < b.threads; i++ {
		b.wg.Add(1)
		go b.listen()
	}

	b.wg.Wait()

	return nil
}

// Stop local broker.
func (b *Beanstalk) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.stop != nil {
		close(b.stop)
	}
}

// Push new job to queue
func (b *Beanstalk) Push(p *jobs.Pipeline, j *jobs.Job) (string, error) {
	data, err := json.Marshal(j)
	if err != nil {
		return "", err
	}

	id, err := b.conn.Put(
		data,
		0,
		j.Options.DelayDuration(),
		j.Options.TimeoutDuration(),
	)

	return fmt.Sprintf("beanstalk:%v", id), nil
}

func (b *Beanstalk) listen() {
	defer b.wg.Done()
	var job *jobs.Job

	for {
		select {
		case <-b.stop:
			return
		default:
			id, body, err := b.conn.Reserve(b.reserve)
			if err != nil {
				// need additional logging
				continue
			}

			err = json.Unmarshal(body, &job)
			if err != nil {
				// need additional logging
				continue
			}

			// local broker does not support job timeouts yet
			err = b.exec(fmt.Sprintf("beanstalk:%v", id), job)
			if err == nil {
				b.conn.Delete(id)
				continue
			}

			if !job.CanRetry() {
				b.conn.Bury(id, 0)
				continue
			}

			// retry
			b.conn.Release(id, 0, job.Options.RetryDuration())
		}
	}
}
