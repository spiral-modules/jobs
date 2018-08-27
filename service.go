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

// EndpointsConfig defines config section related to endpoints configuration.
const EndpointsConfig = "endpoints"

// Handler handles job execution.
type Handler func(j *Job) error

// Service manages job execution and connection to multiple job pipelines.
type Service struct {
	log       *logrus.Logger
	cfg       *Config
	rr        *roadrunner.Server
	container service.Container
	endpoints map[string]Endpoint
}

// NewService creates new service for job handling.
func NewService(log *logrus.Logger, endpoints map[string]Endpoint) *Service {
	return &Service{log: log, endpoints: endpoints}
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
	for name, e := range s.endpoints {
		e.Handler(s.exec)
		s.container.Register(name, e)
	}

	s.container.Init(cfg.Get(EndpointsConfig))

	return true, nil
}

// Serve serves local rr server and creates endpoint association.
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

// Push job to associated endpoint and return job id.
func (s *Service) Push(j *Job) (string, error) {
	endpoint, err := s.getEndpoint(j.Pipeline)
	if err != nil {
		return "", err
	}

	id, err := uuid.NewV4()
	if err != nil {
		return "", err
	}

	j.ID = id.String()

	s.log.Debugf("[jobs] new job `%s`", j.ID)
	return j.ID, endpoint.Push(j)
}

// exec executed job using local RR server. Make sure that service is started.
func (s *Service) exec(j *Job) error {
	ctx, err := j.Context()
	if err != nil {
		s.log.Errorf("[jobs.%s] error `%s`: %s", j.Pipeline, j.ID, err)
		return err
	}

	if _, err := s.rr.Exec(&roadrunner.Payload{Body: j.Body(), Context: ctx}); err != nil {
		s.log.Errorf("[jobs.%s] error `%s`: %s", j.Pipeline, j.ID, err)
		return err
	}

	s.log.Debugf("[jobs.%s] complete `%s`", j.Pipeline, j.ID)
	return nil
}

// return endpoint associated with given pipeline.
func (s *Service) getEndpoint(pipeline string) (Endpoint, error) {
	pipe, ok := s.cfg.Pipelines[pipeline]
	if !ok {
		return nil, fmt.Errorf("undefined pipeline `%s`", pipeline)
	}

	h, ok := s.endpoints[pipe.Endpoint]
	if !ok {
		return nil, fmt.Errorf("undefined endpoint `%s`", pipe)
	}

	return h, nil
}
