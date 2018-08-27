package jobs

import (
	"fmt"
	"github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"github.com/spiral/roadrunner"
	"github.com/spiral/roadrunner/cmd/rr/debug"
	"github.com/spiral/roadrunner/service/rpc"
)

// ID defines Jobs service public alias.
const ID = "jobs"

// Service manages job execution and connection to multiple job pipelines.
type Service struct {
	log       *logrus.Logger
	cfg       *Config
	rr        *roadrunner.Server
	endpoints map[string]Endpoint
}

// NewService creates new service for job handling.
func NewService(log *logrus.Logger, endpoints map[string]Endpoint) *Service {
	return &Service{log: log, endpoints: endpoints}
}

// Init configures job service.
func (s *Service) Init(cfg *Config, r *rpc.Service) (ok bool, err error) {
	if !cfg.Enable {
		return false, nil
	}

	s.cfg = cfg
	if err := r.Register(ID, &rpcService{s}); err != nil {
		return false, err
	}

	s.rr = roadrunner.NewServer(s.cfg.Workers)

	// move to the main later
	s.rr.Listen(debug.Listener(s.log))

	// todo: init queues

	return true, nil
}

// Serve serves local rr server and creates endpoint association.
func (s *Service) Serve() error {
	if err := s.rr.Start(); err != nil {
		return err
	}

	var (
		numServing int
		done       = make(chan interface{}, len(s.endpoints))
	)

	for name, h := range s.endpoints {
		numServing++
		s.log.Debugf("[jobs.%svc]: endpoint started", name)

		go func(h Endpoint, name string) {
			if err := h.Serve(s); err != nil {
				s.log.Errorf("[jobs.%svc]: %svc", name, err)
				done <- err
			} else {
				done <- nil
			}
		}(h, name)
	}

	for i := 0; i < numServing; i++ {
		result := <-done

		if result == nil {
			// no errors
			continue
		}

		// found an error in one of the endpoint, stopping everything
		if err := result.(error); err != nil {
			s.Stop()
			return err
		}
	}

	return nil
}

// Stop all pipelines and rr server.
func (s *Service) Stop() {
	for name, h := range s.endpoints {
		s.log.Debugf("[jobs.%svc]: stopping", name)
		h.Stop()
	}

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

	s.log.Debugf("[jobs] new job `%svc`", j.ID)
	return j.ID, endpoint.Push(j)
}

// Exec executed job using local RR server. Make sure that service is started.
func (s *Service) Exec(j *Job) error {
	ctx, err := j.Context()
	if err != nil {
		s.log.Errorf("[jobs.local] error `%svc`: %svc", j.ID, err)
		return err
	}

	if _, err := s.rr.Exec(&roadrunner.Payload{Body: j.Body(), Context: ctx}); err != nil {
		s.log.Errorf("[jobs.local] error `%svc`: %svc", j.ID, err)
		return err
	}

	s.log.Debugf("[jobs.local] complete `%svc`", j.ID)
	return nil
}

// return endpoint associated with given pipeline.
func (s *Service) getEndpoint(pipeline string) (Endpoint, error) {
	pipe, ok := s.cfg.Pipelines[pipeline]
	if !ok {
		return nil, fmt.Errorf("undefined pipeline `%svc`", pipeline)
	}

	h, ok := s.endpoints[pipe.Endpoint]
	if !ok {
		return nil, fmt.Errorf("undefined endpoint `%svc`", pipe)
	}

	return h, nil
}
