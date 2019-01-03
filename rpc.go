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
func (rpc *rpcServer) Push(j *Job, id *string) (err error) {
	*id, err = rpc.svc.Push(j)
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

// Stop all job consuming.
func (rpc *rpcServer) Stop(stop bool, r *string) (err error) {
	if rpc.svc == nil || rpc.svc.rr == nil {
		return errors.New("jobs server is not running")
	}

	for name, b := range rpc.svc.Brokers {
		if err := b.Consume(rpc.svc.cfg.ConsumedPipelines(name), nil, nil); err != nil {
			return err
		}
	}

	*r = "OK"
	return nil
}

// Resume all job consuming.
func (rpc *rpcServer) Resume(resume bool, r *string) (err error) {
	if rpc.svc == nil || rpc.svc.rr == nil {
		return errors.New("jobs server is not running")
	}

	for name, b := range rpc.svc.Brokers {
		if err := b.Consume(rpc.svc.cfg.ConsumedPipelines(name), rpc.svc.execPool, rpc.svc.error); err != nil {
			return err
		}
	}

	*r = "OK"
	return nil
}

// Workers returns list of active workers and their stats.
func (rpc *rpcServer) Workers(list bool, r *WorkerList) (err error) {
	if rpc.svc == nil || rpc.svc.rr == nil {
		return errors.New("jobs server is not running")
	}

	r.Workers, err = util.ServerState(rpc.svc.rr)
	return err
}

// Stat returns list of active workers and their stats.
func (rpc *rpcServer) Stat(list bool, l *PipelineList) (err error) {
	if rpc.svc == nil || rpc.svc.rr == nil {
		return errors.New("jobs server is not running")
	}

	*l = PipelineList{}
	for name, p := range rpc.svc.cfg.Pipelines {
		stat, err := rpc.svc.Brokers[p.Broker()].Stat(p)
		if err != nil {
			return err
		}

		stat.Name = name
		stat.Broker = p.Broker()
		l.Pipelines = append(l.Pipelines, stat)
	}

	return err
}
