package broker

import (
	"github.com/spiral/jobs"
	"sync"
	"time"
	"github.com/satori/go.uuid"
)

// Local run queue using local goroutines.
type Local struct {
	mu      sync.Mutex
	threads int
	wg      sync.WaitGroup
	queue   chan entry
	exe     jobs.Handler
	err     jobs.ErrorHandler
}

type entry struct {
	id  string
	job *jobs.Job
}

// Init configures local job broker.
func (l *Local) Init() (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.threads = 1
	l.queue = make(chan entry)

	return true, nil
}

// Listen configures broker with list of pipelines to listen and handler function. Local broker groups all pipelines
// together.
func (l *Local) Listen(pipelines []*jobs.Pipeline, exe jobs.Handler, err jobs.ErrorHandler) error {

	//switch {
	//case len(pipelines) == 0:
	//	// no pipelines to handled
	//	return nil
	//
	//case len(pipelines) == 1:
	//	l.threads = pipelines[0].Options.Integer("threads", 1)
	//	if l.threads < 1 {
	//		return errors.New("local queue `thread` number must be 1 or higher")
	//	}
	//
	//default:
	//	return errors.New("local queue handler expects exactly one pipeline")
	//}

	l.exe = exe
	l.err = err
	return nil
}

// Serve local broker.
func (l *Local) Serve() error {
	for i := 0; i <= l.threads; i++ {
		l.wg.Add(1)
		go l.listen()
	}

	l.wg.Wait()
	return nil
}

// Stop local broker.
func (l *Local) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()

	if l.queue != nil {
		close(l.queue)
	}
}

// Push new job to queue
func (l *Local) Push(p *jobs.Pipeline, j *jobs.Job) (string, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return "", err
	}

	// todo: handle stop

	go func() { l.queue <- entry{id: id.String(), job: j} }()

	return id.String(), nil
}

func (l *Local) listen() {
	defer l.wg.Done()
	for q := range l.queue {
		id, job := q.id, q.job

		if job.Options.Delay != 0 {
			time.Sleep(job.Options.DelayDuration())
		}

		// local broker does not support job timeouts yet
		err := l.exe(id, job)
		if err == nil {
			continue
		}

		if !job.CanRetry() {
			l.err(id, job, err)
			continue
		}

		if job.Options.RetryDelay != 0 {
			time.Sleep(job.Options.RetryDuration())
		}

		l.queue <- entry{id: id, job: job}
	}
}
