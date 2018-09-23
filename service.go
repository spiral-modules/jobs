package jobs

import (
	"github.com/sirupsen/logrus"
	"github.com/spiral/roadrunner"
	"github.com/spiral/roadrunner/service/rpc"
	"github.com/spiral/roadrunner/service"
	"github.com/spiral/roadrunner/service/env"
	"fmt"
)

const (
	// ID defines Listen service public alias.
	ID = "jobs"

	// indicates that job service is running
	jobsKey = "rr_jobs"
)

// BrokersConfig defines config section related to Brokers configuration.
const BrokersConfig = "Brokers"

// Listen handles job execution.
type Handler func(id string, j *Job) error

// Listen handles job execution.
type ErrorHandler func(id string, j *Job, err error) error

type emptyConfig struct{}

// Get is doing nothing.
func (e *emptyConfig) Get(service string) service.Config {
	return nil
}

// Unmarshal is doing nothing.
func (e *emptyConfig) Unmarshal(out interface{}) error {
	return nil
}

// Service manages job execution and connection to multiple job pipelines.
type Service struct {
	// Brokers define list of available brokers.
	Brokers map[string]Broker
	cfg     *Config
	env     env.Environment
	log     *logrus.Logger
	brokers service.Container
	rr      *roadrunner.Server
}

// Init configures job service.
func (s *Service) Init(
	c service.Config,
	l *logrus.Logger,
	r *rpc.Service,
	e env.Environment,
) (ok bool, err error) {
	s.cfg = &Config{}
	s.log = l
	s.env = e

	if err := s.cfg.Hydrate(c); err != nil {
		return false, err
	}

	if r != nil {
		if err := r.Register(ID, &rpcService{s}); err != nil {
			return false, err
		}
	}

	s.rr = roadrunner.NewServer(s.cfg.Workers)

	// we are going to keep all handlers withing the brokers
	// so we can easier manage their state and configuration
	s.brokers = service.NewContainer(s.log)
	for name, e := range s.Brokers {
		pipes := make([]*Pipeline, 0)
		for _, p := range s.cfg.Pipelines {
			if p.Broker == name {
				pipes = append(pipes, p)
			}
		}

		if err := e.Listen(pipes, s.exec, s.error); err != nil {
			return false, err
		}

		s.brokers.Register(name, e)
	}

	cfg := c.Get(BrokersConfig)
	if cfg == nil {
		cfg = &emptyConfig{}
	}

	if err := s.brokers.Init(cfg); err != nil {
		return false, err
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

		s.cfg.Workers.SetEnv(jobsKey, "true")
	}

	if err := s.rr.Start(); err != nil {
		return err
	}
	defer s.rr.Stop()

	return s.brokers.Serve()
}

// Stop all pipelines and rr server.
func (s *Service) Stop() {
	s.brokers.Stop()
}

// Push job to associated broker and return job id.
func (s *Service) Push(j *Job) (string, error) {
	p, b, err := s.getPipeline(j.Job)
	if err != nil {
		return "", err
	}

	id, err := b.Push(p, j)

	if err != nil {
		s.log.Errorf("[jobs] `%s`: %s", j.Job, err.Error())
	} else {
		s.log.Debugf("[jobs] push `%s`.`%s`", j.Job, id)
	}

	return id, err
}

// exec executed job using local RR server. Make sure that service is started.
func (s *Service) exec(id string, j *Job) error {
	ctx, err := j.Context(id)
	if err != nil {
		s.log.Errorf("[jobs] fail `%s`.`%s`: %s", j.Job, id, err)
		return err
	}

	j.Attempt++

	_, err = s.rr.Exec(&roadrunner.Payload{Body: j.Body(), Context: ctx})
	if err == nil {
		s.log.Debugf("[jobs] done `%s`.`%s`", j.Job, id)
		return nil
	}

	// broker can handle retry or register job as errored
	return err
}

// error must be invoked when job is declared as failed.
func (s *Service) error(id string, j *Job, err error) error {
	s.log.Errorf("[jobs] error `%s`.`%s`: %s", j.Job, id, err.Error())
	return err
}

// return broker associated with given pipeline.
func (s *Service) getPipeline(job string) (*Pipeline, Broker, error) {
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
