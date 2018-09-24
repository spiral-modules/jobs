package beanstalk

import (
	"encoding/json"
	"fmt"
	"github.com/spiral/jobs"
	"github.com/beanstalkd/go-beanstalk"
	"sync"
	"time"
)

// Broker run jobs using Broker service.
type Broker struct {
	cfg         *Config
	mu          sync.Mutex
	stop        chan interface{}
	conn        *beanstalk.Conn
	tubes       map[*jobs.Pipeline]*Tube
	tubeset     *beanstalk.TubeSet
	handlerPool chan jobs.Handler
	err         jobs.ErrorHandler
}

// Listen configures broker with list of tubes to listen and handler function. Local broker groups all tubes
// together.
func (b *Broker) Listen(pipelines []*jobs.Pipeline, pool chan jobs.Handler, err jobs.ErrorHandler) error {
	b.tubes = make(map[*jobs.Pipeline]*Tube)
	for _, p := range pipelines {
		if err := b.registerTube(p); err != nil {
			return err
		}
	}

	b.handlerPool = pool
	b.err = err
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

	var names []string
	for _, t := range b.tubes {
		t.Tube.Conn = b.conn

		if t.Listen {
			names = append(names, t.Name)
		}
	}

	if len(names) != 0 {
		b.listen(beanstalk.NewTubeSet(b.conn, names...))
	}
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

	return fmt.Sprintf("%v", id), err
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
func (b *Broker) listen(t *beanstalk.TubeSet) {
	var job *jobs.Job
	var handler jobs.Handler

	for {
		select {
		case <-b.stop:
			return
		default:
			id, body, err := t.Reserve(time.Duration(b.cfg.Reserve) * time.Second)
			if err != nil {
				continue
			}

			err = json.Unmarshal(body, &job)
			if err != nil {
				// need additional logging
				continue
			}

			handler = <-b.handlerPool
			go func() {
				err = handler(fmt.Sprintf("%v", id), job)
				b.handlerPool <- handler

				if err == nil {
					b.conn.Delete(id)
					return
				}

				if !job.CanRetry() {
					b.conn.Delete(id)
					b.err(fmt.Sprintf("%v", id), job, err)
					return
				}

				// retry
				b.conn.Release(id, 0, job.Options.RetryDuration())
			}()
		}
	}
}
