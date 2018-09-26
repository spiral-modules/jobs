package jobs

import (
	"errors"
	"github.com/spiral/roadrunner/util"
)

type rpcServer struct{ svc *Service }

// WorkerList contains list of workers.
type WorkerList struct {
	// Workers is list of workers.
	Workers []*util.State `json:"workers"`
}

// Push job to the queue.
func (s *rpcServer) Push(j *Job, id *string) (err error) {
	*id, err = s.svc.Push(j)
	return
}

// Reset resets underlying RR worker pool and restarts all of it's workers.
func (rpc *rpcServer) Reset(reset bool, r *string) error {
	if rpc.svc == nil || rpc.svc.rr == nil {
		return errors.New("jobs server is not running")
	}

	*r = "OK"
	return rpc.svc.rr.Reset()
}

// Workers returns list of active workers and their stats.
func (rpc *rpcServer) Workers(list bool, r *WorkerList) (err error) {
	if rpc.svc == nil || rpc.svc.rr == nil {
		return errors.New("jobs server is not running")
	}

	r.Workers, err = util.ServerState(rpc.svc.rr)
	return err
}
