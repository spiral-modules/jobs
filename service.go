package jobs

import (
	"fmt"
	"github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"github.com/spiral/roadrunner"
	"github.com/spiral/roadrunner/service/rpc"
	"github.com/spiral/roadrunner/service"
)

// ID defines Jobs service public alias.
const ID = "jobs"

// BrokersConfig defines config section related to brokers configuration.
const BrokersConfig = "brokers"

// Handle handles job execution.
type Handler func(j *Job) error

// Handle handles job execution.
type ErrorHandler func(j *Job, err error) error

// Service manages job execution and connection to multiple job pipelines.
type Service struct {
	log       *logrus.Logger
	cfg       *Config
	rr        *roadrunner.Server
	container service.Container
	brokers   map[string]Broker
}

// NewService creates new service for job handling.
func NewService(log *logrus.Logger, brokers map[string]Broker) *Service {
	return &Service{log: log, brokers: brokers}
}

// Init configures job service.
func (s *Service) Init(cfg service.Config, r *rpc.Service) (ok bool, err error) {
	config := &Config{}
	if err := config.Hydrate(cfg); err != nil {
		return false, err
	}

	if !config.Enable {
		return false, nil
	}

	s.cfg = config
	if err := r.Register(ID, &rpcService{s}); err != nil {
		return false, err
	}

	s.rr = roadrunner.NewServer(s.cfg.Workers)

	// we are going to keep all handlers withing the container
	// so we can easier manage their state and configuration
	s.container = service.NewContainer(s.log)
	for name, e := range s.brokers {
		pipes := make([]*Pipeline, 0)
		for _, p := range s.cfg.Pipelines {
			if p.Broker == name {
				pipes = append(pipes, p)
			}
		}

		if err := e.Handle(pipes, s.exec, s.error); err != nil {
			return false, err
		}

		s.container.Register(name, e)
	}

	s.container.Init(cfg.Get(BrokersConfig))

	return true, nil
}

// Serve serves local rr server and creates broker association.
func (s *Service) Serve() error {
	if err := s.rr.Start(); err != nil {
		return err
	}

	return s.container.Serve()
}

// Stop all pipelines and rr server.
func (s *Service) Stop() {
	s.container.Stop()

	s.rr.Stop()
	s.log.Debugf("[jobs]: stopped")
}

// Push job to associated broker and return job id.
func (s *Service) Push(j *Job) (string, error) {
	p, b, err := s.getPipeline(j.Pipeline)
	if err != nil {
		return "", err
	}

	id, err := uuid.NewV4()
	if err != nil {
		return "", err
	}

	j.ID = id.String()

	s.log.Debugf("[jobs] new job `%s`", j.ID)
	return j.ID, b.Push(p, j)
}

// exec executed job using local RR server. Make sure that service is started.
func (s *Service) exec(j *Job) error {
	ctx, err := j.Context()
	if err != nil {
		s.log.Errorf("[jobs] error `%s`: %s", j.ID, err)
		return err
	}

	j.Attempt++
	_, err = s.rr.Exec(&roadrunner.Payload{Body: j.Body(), Context: ctx})
	if err == nil {
		s.log.Debugf("[jobs] complete `%s`", j.ID)
		return nil
	}

	return err
}

// error must be invoked when job is declared as failed.
func (s *Service) error(j *Job, err error) error {
	s.log.Errorf("[jobs] error `%s`: %s", j.ID, err.Error())
	return err
}

// return broker associated with given pipeline.
func (s *Service) getPipeline(pipeline string) (*Pipeline, Broker, error) {
	pipe, ok := s.cfg.Pipelines[pipeline]
	if !ok {
		return nil, nil, fmt.Errorf("undefined pipeline `%s`", pipeline)
	}

	h, ok := s.brokers[pipe.Broker]
	if !ok {
		return nil, nil, fmt.Errorf("undefined broker `%s`", pipe)
	}

	return pipe, h, nil
}
