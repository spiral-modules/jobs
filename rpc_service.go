package jobs

import (
	"github.com/spiral/jobs/handler"
)

type rpcService struct {
	s *Service
}

func (s *rpcService) Push(j *handler.Job, id *string) error {
	h, err := s.s.getHandler(j.Pipeline)
	if err != nil {
		return err
	}

	*id, err = h.Handle(j)

	return err
}
