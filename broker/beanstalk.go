package broker

import (
	"github.com/spiral/jobs"
	"sync"
	"github.com/beanstalkd/go-beanstalk"
)

// Beanstalk run jobs using Beanstalk service.
type Beanstalk struct {
	mu   sync.Mutex
	conn *beanstalk.Conn
	wg   sync.WaitGroup
	exec jobs.Handler
	fail jobs.ErrorHandler
}

// Init configures local job broker.
func (b *Beanstalk) Init(cfg *BeanstalkConfig) (bool, error) {
	if !cfg.Enable {
		return false, nil
	}

	return true, nil
}

// Handle configures broker with list of pipelines to listen and handler function. Local broker groups all pipelines
// together.
func (b *Beanstalk) Handle(pipelines []*jobs.Pipeline, h jobs.Handler, f jobs.ErrorHandler) error {
	//switch {
	//case len(pipelines) < 1:
	//	// no pipelines to handleThread
	//	return nil
	//
	//case len(pipelines) == 1:
	//	b.threads = pipelines[0].Options.Integer("threads", 1)
	//	if b.threads < 1 {
	//		return errors.New("local queue handler threads must be 1 or higher")
	//	}
	//
	//default:
	//	return errors.New("local queue handler expects exactly one pipeline")
	//}

	b.exec = h
	b.fail = f
	return nil
}

// Push new job to queue
func (b *Beanstalk) Push(p *jobs.Pipeline, j *jobs.Job) error {
	//go func() { b.jobs <- j }()

	return nil
}

// Serve local broker.
func (b *Beanstalk) Serve() error {
	//conn, err := beanstalk.Dial("tcp", "127.0.0.1:11300")
	//if err != nil {
//		return err
	//}

	//b.conn = conn
	//b.conn.Put()

	//for i := 0; i < b.threads; i++ {
	//	b.wg.Add(1)
	//	go b.listen()
	//}

	b.wg.Wait()
	b.conn.Close()

	return nil
}

// Stop local broker.
func (b *Beanstalk) Stop() {
	//b.mu.Lock()
	//defer b.mu.Unlock()
	//
	//if b.jobs != nil {
	//	close(b.jobs)
	//	b.jobs = nil
	//}

	b.conn.Close()
}

func (b *Beanstalk) listen() {
	//defer b.wg.Done()
	//for j := range b.jobs {
	//	if j == nil {
	//		return
	//	}
	//
	//	if j.Options != nil && j.Options.Delay != nil {
	//		time.Sleep(time.Second * time.Duration(*j.Options.Delay))
	//	}
	//
	//	// local broker does not have a way to confirm job re-execution
	//	if err := b.exec(j); err != nil {
	//		if j.CanRetry() {
	//			b.jobs <- j
	//		} else {
	//			b.fail(j, err)
	//		}
	//	}
	//}
}
