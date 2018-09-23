package beanstalk

import (
	"github.com/spiral/jobs"
	"github.com/xuri/aurora/beanstalk"
	"sync"
	"fmt"
	"encoding/json"
)

// Broker run jobs using Broker service.
type Broker struct {
	cfg   *Config
	mu    sync.Mutex
	stop  chan interface{}
	conn  *beanstalk.Conn
	wg    sync.WaitGroup
	tubes map[*jobs.Pipeline]*Tube
	exe   jobs.Handler
	err   jobs.ErrorHandler
}

// Listen configures broker with list of tubes to listen and handler function. Local broker groups all tubes
// together.
func (b *Broker) Listen(pipelines []*jobs.Pipeline, h jobs.Handler, f jobs.ErrorHandler) error {
	b.tubes = make(map[*jobs.Pipeline]*Tube)
	for _, p := range pipelines {
		if err := b.registerTube(p); err != nil {
			return err
		}
	}

	b.exe = h
	b.err = f
	return nil
}

// Init configures local job broker.
func (b *Broker) Init(cfg *Config) (bool, error) {
	b.cfg = cfg
	return true, nil
}

// Serve tubes.
func (b *Broker) Serve() error {
	conn, err := b.cfg.Conn()
	if err != nil {
		return err
	}
	defer conn.Close()

	b.conn = conn

	b.mu.Lock()
	b.stop = make(chan interface{})
	b.mu.Unlock()

	for _, t := range b.tubes {
		t.Tube.Conn = b.conn

		if t.Listen {
			for i := 0; i < t.Threads; i++ {
				b.wg.Add(1)
				go b.listen(t)
			}
		}
	}

	b.wg.Wait()
	<-b.stop

	return nil
}

// Stop serving.
func (b *Broker) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.stop != nil {
		close(b.stop)
	}
}

// Push new job to queue
func (b *Broker) Push(p *jobs.Pipeline, j *jobs.Job) (string, error) {
	data, err := json.Marshal(j)
	if err != nil {
		return "", err
	}

	id, err := b.tubes[p].Put(
		data,
		0,
		j.Options.DelayDuration(),
		j.Options.TimeoutDuration(),
	)

	if err != nil {
		return "", err
	}

	return fmt.Sprintf("beanstalk:%v", id), err
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

// listen jobs from given tube
func (b *Broker) listen(t *Tube) {
	defer b.wg.Done()
	var job *jobs.Job

	for {
		select {
		case <-b.stop:
			return
		default:
			id, body, err := t.PeekReady()
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
			err = b.exe(fmt.Sprintf("beanstalk:%v", id), job)
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
