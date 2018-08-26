package jobs

import (
	"github.com/sirupsen/logrus"
	"github.com/spiral/roadrunner/service/rpc"
	"github.com/spiral/jobs/handler"
)

const ID = "jobs"

type Service struct {
	Logger   *logrus.Logger
	cfg      *Config
	handlers map[string]Handler
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

		go func(h Handler, name string) {
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
	s.handlers = make(map[string]Handler)

	h, err := handler.NewLocal(s.cfg.Handlers.Local)
	if err != nil {
		return err
	}

	s.handlers["local"] = h

	return nil
}

func (s *Service) getHandler(handler string) Handler {
	return nil
}
