package jobs

import (
	"github.com/sirupsen/logrus"
	"github.com/spiral/roadrunner/service/rpc"
	"github.com/spiral/jobs/handler"
	"fmt"
)

const ID = "jobs"

type Service struct {
	Logger   *logrus.Logger
	cfg      *Config
	handlers map[string]handler.Handler
}

func (s *Service) Init(cfg *Config, r *rpc.Service) (ok bool, err error) {
	if !cfg.Enabled {
		return false, nil
	}

	s.cfg = cfg
	if err := r.Register(ID, &rpcService{s}); err != nil {
		return false, err
	}

	return true, s.initHandlers()
}

func (s *Service) Serve() error {
	var (
		numServing int
		done       = make(chan interface{}, len(s.handlers))
	)

	for name, h := range s.handlers {
		numServing++
		s.Logger.Debugf("[jobs.%s]: handler started", name)

		go func(h handler.Handler, name string) {
			if err := h.Serve(); err != nil {
				s.Logger.Errorf("[jobs.%s]: %s", name, err)
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

		// found an error in one of the services, stopping the rest of running services.
		if err := result.(error); err != nil {
			s.Stop()
			return err
		}
	}

	return nil
}

func (s *Service) Stop() {
	for name, h := range s.handlers {
		h.Stop()
		s.Logger.Debugf("[jobs.%s]: stopped", name)
	}
}

func (s *Service) initHandlers() error {
	s.handlers = make(map[string]handler.Handler)

	h, err := handler.LocalHandler(s.cfg.Handlers.Local, s.Logger)
	if err != nil {
		return err
	}

	s.handlers["local"] = h

	return nil
}

func (s *Service) getHandler(pipeline string) (handler.Handler, error) {
	target, ok := s.cfg.Pipelines[pipeline]
	if !ok {
		return nil, fmt.Errorf("undefined pipeline %s", pipeline)
	}

	h, ok := s.handlers[target]
	if !ok {
		return nil, fmt.Errorf("undefined handler %s", target)
	}

	return h, nil
}
