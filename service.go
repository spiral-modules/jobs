package jobs

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spiral/roadrunner"
	"github.com/spiral/roadrunner/service"
	"github.com/spiral/roadrunner/service/env"
	"github.com/spiral/roadrunner/service/rpc"
)

// ID defines Listen service public alias.
const ID = "jobs"

// Listen handles job execution.
type Handler func(id string, j *Job) error

// Listen handles job execution.
type ErrorHandler func(id string, j *Job, err error)

// Service manages job execution and connection to multiple job pipelines.
type Service struct {
	// Brokers define list of available container.
	Brokers   map[string]Broker
	cfg       *Config
	env       env.Environment
	logger    *logrus.Logger
	lsns      []func(event int, ctx interface{})
	rr        *roadrunner.Server
	container service.Container
	execs     chan Handler
}

// AddListener attaches server event watcher.
func (s *Service) AddListener(l func(event int, ctx interface{})) {
	s.lsns = append(s.lsns, l)
}

// Init configures job service.
func (s *Service) Init(c service.Config, l *logrus.Logger, r *rpc.Service, e env.Environment) (ok bool, err error) {
	s.cfg = &Config{}
	s.logger = l
	s.env = e

	if err := s.cfg.Hydrate(c); err != nil {
		return false, err
	}

	// Configuring worker pools
	s.execs = make(chan Handler, s.cfg.Workers.Pool.NumWorkers)

	for i := int64(0); i < s.cfg.Workers.Pool.NumWorkers; i++ {
		s.execs <- s.exec
	}

	if r != nil {
		if err := r.Register(ID, &rpcServer{s}); err != nil {
			return false, err
		}
	}

	s.rr = roadrunner.NewServer(s.cfg.Workers)

	// we are going to keep all handlers withing the container
	// so we can easier manage their state and configuration
	s.container = service.NewContainer(s.logger)
	for name, e := range s.Brokers {
		pipes := make([]*Pipeline, 0)
		for _, p := range s.cfg.Pipelines {
			if p.Broker == name {
				pipes = append(pipes, p)
			}
		}

		if err := e.Listen(pipes, s.execs, s.error); err != nil {
			return false, err
		}

		s.container.Register(name, e)
	}

	cfg := c.Get(BrokerConfig)
	if cfg == nil {
		cfg = &emptyConfig{}
	}

	if err := s.container.Init(cfg); err != nil {
		// container will show error
		return false, nil
	}

	return true, nil
}

// Serve serves local rr server and creates broker association.
func (s *Service) Serve() error {
	if s.env != nil {
		values, err := s.env.GetEnv()
		if err != nil {
			return err
		}

		for k, v := range values {
			s.cfg.Workers.SetEnv(k, v)
		}

		s.cfg.Workers.SetEnv("rr_jobs", "true")
	}

	s.rr.Listen(s.throw)

	if err := s.rr.Start(); err != nil {
		return err
	}

	return s.container.Serve()
}

// Stop all pipelines and rr server.
func (s *Service) Stop() {
	s.container.Stop()
	s.rr.Stop()
}

// Push job to associated broker and return job id.
func (s *Service) Push(j *Job) (string, error) {
	p, b, err := s.findPipeline(j.Job)
	if err != nil {
		return "", err
	}

	id, err := b.Push(p, j)

	if err != nil {
		s.throw(EventPushError, &ErrorEvent{Job: j, Error: err})
	} else {
		s.throw(EventJobAdded, &JobEvent{ID: id, Job: j})
	}

	return id, err
}

// exec executed job using local RR server. Make sure that service is started.
func (s *Service) exec(id string, j *Job) error {
	ctx, err := j.Context(id)
	if err != nil {
		s.error(id, j, err)
		return err
	}

	_, err = s.rr.Exec(&roadrunner.Payload{Body: j.Body(), Context: ctx})
	if err == nil {
		s.throw(EventJobComplete, &JobEvent{ID: id, Job: j})
		return nil
	}

	// broker can handle retry or register job as errored
	return err
}

// error must be invoked when job is declared as failed.
func (s *Service) error(id string, j *Job, err error) {
	s.throw(EventJobError, &ErrorEvent{ID: id, Job: j, Error: err})
}

// return broker associated with given pipeline.
func (s *Service) findPipeline(job string) (*Pipeline, Broker, error) {
	var pipe *Pipeline
	for _, p := range s.cfg.Pipelines {
		if p.Has(job) {
			pipe = p
			break
		}
	}

	if pipe == nil {
		return nil, nil, fmt.Errorf("unable to find pipeline for `%s`", job)
	}

	h, ok := s.Brokers[pipe.Broker]
	if !ok {
		return nil, nil, fmt.Errorf("undefined broker `%s`", pipe.Broker)
	}

	return pipe, h, nil
}

// throw handles service, server and pool events.
func (s *Service) throw(event int, ctx interface{}) {
	for _, l := range s.lsns {
		l(event, ctx)
	}

	if event == roadrunner.EventServerFailure {
		// underlying rr server is dead
		s.Stop()
	}
}
