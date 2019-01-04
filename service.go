package jobs

import (
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spiral/roadrunner"
	"github.com/spiral/roadrunner/service"
	"github.com/spiral/roadrunner/service/env"
	"github.com/spiral/roadrunner/service/rpc"
	"sync"
)

// ID defines public service name.
const ID = "jobs"

// Service wraps roadrunner container and manage set of parent within it.
type Service struct {
	// Associated parent
	Brokers   map[string]Broker
	cfg       *Config
	env       env.Environment
	log       *logrus.Logger
	execPool  chan Handler
	lsns      []func(event int, ctx interface{})
	rr        *roadrunner.Server
	services  service.Container
	mu        sync.Mutex
	consuming map[*Pipeline]bool
}

// AddListener attaches server event watcher.
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

	s.services = service.NewContainer(l)

	s.mu.Lock()
	s.consuming = make(map[*Pipeline]bool)
	for _, p := range s.Pipelines() {
		s.consuming[p] = false
	}
	s.mu.Unlock()

	for name, b := range s.Brokers {
		// registering pipelines and handlers
		if err := b.Register(s.Pipelines().Broker(name)); err != nil {
			return false, err
		}

		s.services.Register(name, b)
	}

	// init all broker configs
	if err := s.services.Init(s.cfg); err != nil {
		return false, err
	}

	return true, nil
}

// serve serves local rr server and creates broker association.
func (s *Service) Serve() error {
	// ensure that workers aware of running within jobs
	if s.env != nil {
		values, err := s.env.GetEnv()
		if err != nil {
			return err
		}

		for k, v := range values {
			s.cfg.Workers.SetEnv(k, v)
		}
	}

	s.cfg.Workers.SetEnv("rr_jobs", "true")
	s.rr.Listen(s.throw)

	if err := s.rr.Start(); err != nil {
		return err
	}
	defer s.rr.Stop()

	for broker := range s.Brokers {
		for _, p := range s.Pipelines().Broker(broker).Names(s.cfg.Consume...) {
			if err := s.Consume(p, s.execPool, s.error); err != nil {
				return err
			}
		}
	}

	return s.services.Serve()
}

// stop all pipelines and rr server.
func (s *Service) Stop() {
	// explicitly stop all consuming
	for broker := range s.Brokers {
		for _, p := range s.Pipelines().Broker(broker) {
			if err := s.Consume(p, nil, nil); err != nil {
				s.throw(EventBrokerError, err)
			}
		}
	}

	s.services.Stop()
}

// Push job to associated broker and return job id.
func (s *Service) Push(j *Job) (string, error) {
	pipe, err := s.cfg.MapPipeline(j)
	if err != nil {
		return "", err
	}

	broker, ok := s.Brokers[pipe.Broker()]
	if !ok {
		return "", fmt.Errorf("undefined broker `%s`", pipe.Broker())
	}

	id, err := broker.Push(pipe, j)

	if err != nil {
		s.throw(EventPushError, &ErrorEvent{Job: j, Error: err})
	} else {
		s.throw(EventPushComplete, &JobEvent{ID: id, Job: j})
	}

	return id, err
}

// Pipelines return all service pipelines.
func (s *Service) Pipelines() Pipelines {
	return s.cfg.Pipelines
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

	stat.Pipeline = pipe.Name()
	stat.Broker = pipe.Broker()

	return stat, err
}

// Consuming enables or disables pipeline consuming using given handlers.
func (s *Service) Consume(pipe *Pipeline, execPool chan Handler, errHandler ErrorHandler) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if execPool != nil {
		if s.consuming[pipe] {
			return nil
		}

		s.throw(EventPipelineConsume, pipe)
		s.consuming[pipe] = true
	} else {
		if !s.consuming[pipe] {
			return nil
		}

		s.throw(EventPipelineStop, pipe)
		s.consuming[pipe] = false
	}

	broker, ok := s.Brokers[pipe.Broker()]
	if !ok {
		return fmt.Errorf("undefined broker `%s`", pipe.Broker())
	}

	if err := broker.Consume(pipe, execPool, errHandler); err != nil {
		return err
	}

	return nil
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
