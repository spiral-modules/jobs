package jobs

import "github.com/sirupsen/logrus"

type rpcService struct {
	s *Service
}

type Job struct {
	Job      string `json:"job"`
	Pipeline string `json:"pipeline"`
	Payload  interface{}
	Options struct {
		Delay *int `json:"delay"`
	}
}

func (s *rpcService) Push(j *Job, id *string) error {
	*id = j.Job

	logrus.Println(*id)
	return nil
}
