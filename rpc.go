package jobs

type rpcService struct {
	s *Service
}

// Push job to the queue.
func (s *rpcService) Push(j *Job, id *string) (err error) {
	*id, err = s.s.Push(j)
	return
}
