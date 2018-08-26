package jobs

import (
	"github.com/sirupsen/logrus"
	"github.com/spiral/roadrunner/service/rpc"
)

const ID = "jobs"

type Service struct {
	Logger *logrus.Logger
	cfg    *Config
}

func (s *Service) Init(cfg *Config, r *rpc.Service) (ok bool, err error) {
	if !cfg.Enabled {
		return false, nil
	}

	s.cfg = cfg
	if err := r.Register(ID, &rpcService{s}); err != nil {
		return false, err
	}

	return true, nil
}

func (s *Service) Serve() error {
	return nil
}

func (s *Service) Stop() {

}
