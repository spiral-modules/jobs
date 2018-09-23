package jobs

type rpcService struct {
	svc *Service
}

// Push job to the queue.
func (s *rpcService) Push(j *Job, id *string) (err error) {
	*id, err = s.svc.Push(j)
	return
}
