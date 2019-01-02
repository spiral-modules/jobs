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

// PipelineList contains list of pipeline stats.
type PipelineList struct {
	// Pipelines is list of pipeline stats.
	Pipelines []*Stat `json:"pipelines"`
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

// Workers returns list of active workers and their stats.
func (rpc *rpcServer) Stat(list bool, l *PipelineList) (err error) {
	if rpc.svc == nil || rpc.svc.rr == nil {
		return errors.New("jobs server is not running")
	}

	*l = PipelineList{}
	for _, p := range rpc.svc.cfg.Pipelines {
		stat, err := rpc.svc.Brokers[p.Broker].Stat(p)
		if err != nil {
			return err
		}

		stat.Broker = p.Broker
		l.Pipelines = append(l.Pipelines, stat)
	}

	return err
}
