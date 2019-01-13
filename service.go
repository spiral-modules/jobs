package jobs

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spiral/roadrunner"
	"github.com/spiral/roadrunner/service"
	"github.com/spiral/roadrunner/service/env"
	"github.com/spiral/roadrunner/service/rpc"
	"sync"
	"time"
)

// String defines public service name.
const ID = "jobs"

// Service wraps roadrunner container and manage set of parent within it.
type Service struct {
	// Associated parent
	Brokers map[string]Broker

	cfg       *Config
	env       env.Environment
	log       *logrus.Logger
	lsns      []func(event int, ctx interface{})
	rr        *roadrunner.Server
	execPool  chan Handler
	services  service.Container
	mu        sync.Mutex
	consuming map[*Pipeline]bool
}

// Listen attaches event watcher.
func (s *Service) AddListener(l func(event int, ctx interface{})) {
	s.lsns = append(s.lsns, l)
}

// Init configures job service.
func (s *Service) Init(c service.Config, l *logrus.Logger, r *rpc.Service, e env.Environment) (ok bool, err error) {
	s.cfg = &Config{}
	s.env = e
	s.log = l

	if err := s.cfg.Hydrate(c); err != nil {
		return false, err
	}

	// configuring worker pools
	s.execPool = make(chan Handler, s.cfg.Workers.Pool.NumWorkers)
	for i := int64(0); i < s.cfg.Workers.Pool.NumWorkers; i++ {
		s.execPool <- s.exec
	}

	if r != nil {
		if err := r.Register(ID, &rpcServer{s}); err != nil {
			return false, err
		}
	}

	s.rr = roadrunner.NewServer(s.cfg.Workers)

	s.mu.Lock()
	s.consuming = make(map[*Pipeline]bool)
	for _, p := range s.Pipelines() {
		s.consuming[p] = false
	}
	s.mu.Unlock()

	// run all brokers in nested container
	s.services = service.NewContainer(l)
	for name, b := range s.Brokers {
		s.services.Register(name, b)
		if eb, ok := b.(EventProvider); ok {
			eb.Listen(s.throw)
		}
	}

	// init all broker configs
	if err := s.services.Init(s.cfg); err != nil {
		return false, err
	}

	// register all pipelines
	for name, b := range s.Brokers {
		for _, pipe := range s.Pipelines().Broker(name) {
			if err := b.Register(pipe); err != nil {
				return false, err
			}
		}
	}

	return true, nil
}

// Serve serves local rr server and creates broker association.
func (s *Service) Serve() error {
	// ensure that workers aware of running within jobs
	if s.env != nil {
		if err := s.env.Copy(s.cfg.Workers); err != nil {
			return err
		}
	}

	s.cfg.Workers.SetEnv("rr_jobs", "true")
	s.rr.Listen(s.throw)

	if err := s.rr.Start(); err != nil {
		return err
	}
	defer s.rr.Stop()

	// start consuming of all the pipelines
	for _, p := range s.Pipelines().Names(s.cfg.Consume...) {
		if err := s.Consume(p, s.execPool, s.error); err != nil {
			return err
		}
	}

	return s.services.Serve()
}

// Stop all pipelines and rr server.
func (s *Service) Stop() {
	wg := sync.WaitGroup{}
	for _, p := range s.Pipelines().Names(s.cfg.Consume...).Reverse() {
		wg.Add(1)

		go func(p *Pipeline) {
			defer wg.Done()
			if err := s.Consume(p, nil, nil); err != nil {
				s.throw(EventPipelineError, &PipelineError{Pipeline: p, Caused: err})
			}
		}(p)
	}

	wg.Wait()
	s.services.Stop()
}

// Push job to associated broker and return job id.
func (s *Service) Push(job *Job) (string, error) {
	pipe, opt, err := s.cfg.FindPipeline(job)
	if err != nil {
		return "", err
	}

	if opt != nil {
		job.Options.Merge(opt)
	}

	broker, ok := s.Brokers[pipe.Broker()]
	if !ok {
		return "", fmt.Errorf("undefined broker `%s`", pipe.Broker())
	}

	id, err := broker.Push(pipe, job)

	if err != nil {
		s.throw(EventPushError, &JobError{Job: job, Caused: err})
	} else {
		s.throw(EventPushComplete, &JobEvent{ID: id, Job: job})
	}

	return id, err
}

// Pipelines return all service pipelines.
func (s *Service) Pipelines() Pipelines {
	return s.cfg.pipelines
}

// Stat returns list of active workers and their stats.
func (s *Service) Stat(pipe *Pipeline) (stat *Stat, err error) {
	b, ok := s.Brokers[pipe.Broker()]
	if !ok {
		return nil, fmt.Errorf("undefined broker `%s`", pipe.Broker())
	}

	stat, err = b.Stat(pipe)
	if err != nil {
		return nil, err
	}

	if stat == nil {
		return nil, fmt.Errorf("can't read %s", pipe.Name())
	}

	stat.Pipeline = pipe.Name()
	stat.Broker = pipe.Broker()

	return stat, err
}

// Consume enables or disables pipeline consuming using given handlers.
func (s *Service) Consume(pipe *Pipeline, execPool chan Handler, errHandler ErrorHandler) error {
	s.mu.Lock()

	if execPool != nil {
		if s.consuming[pipe] {
			s.mu.Unlock()
			return nil
		}

		s.throw(EventPipelineConsume, pipe)
		s.consuming[pipe] = true
	} else {
		if !s.consuming[pipe] {
			s.mu.Unlock()
			return nil
		}

		s.throw(EventPipelineStop, pipe)
		s.consuming[pipe] = false
	}

	broker, ok := s.Brokers[pipe.Broker()]
	if !ok {
		s.mu.Unlock()
		return fmt.Errorf("undefined broker `%s`", pipe.Broker())
	}
	s.mu.Unlock()

	if err := broker.Consume(pipe, execPool, errHandler); err != nil {
		return err
	}

	if execPool != nil {
		s.throw(EventPipelineConsuming, pipe)
	} else {
		s.throw(EventPipelineStopped, pipe)
	}

	return nil
}

// exec executed job using local RR server. Make sure that service is started.
func (s *Service) exec(id string, j *Job) error {
	start := time.Now()
	s.throw(EventJobReceived, &JobEvent{ID: id, Job: j, start: start})

	_, err := s.rr.Exec(&roadrunner.Payload{
		Body:    j.Body(),
		Context: j.Context(id),
	})

	if err == nil {
		s.throw(EventJobComplete, &JobEvent{ID: id, Job: j, start: start, elapsed: time.Since(start)})
	} else {
		s.throw(EventJobError, &JobError{ID: id, Job: j, Caused: err, start: start, elapsed: time.Since(start)})
	}

	return err
}

// register died job, can be used to move to fallback queue or log
func (s *Service) error(id string, j *Job, err error) {
	// nothing for now
}

// throw handles service, server and pool events.
func (s *Service) throw(event int, ctx interface{}) {
	for _, l := range s.lsns {
		l(event, ctx)
	}

	if event == roadrunner.EventServerFailure {
		// underlying rr server is dead, stop everything
		s.Stop()
	}
}
