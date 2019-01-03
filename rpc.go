package jobs

import (
	"errors"
	"github.com/spiral/roadrunner/util"
)

type rpcServer struct{ s *Service }

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
func (r *rpcServer) Push(j *Job, id *string) (err error) {
	*id, err = r.s.Push(j)
	return
}

// Reset resets underlying RR worker pool and restarts all of it's workers.
func (r *rpcServer) Reset(reset bool, w *string) error {
	if r.s == nil || r.s.rr == nil {
		return errors.New("jobs server is not running")
	}

	*w = "OK"
	return r.s.rr.Reset()
}

// Stop all job consuming.
func (r *rpcServer) Stop(stop bool, w *string) (err error) {
	if r.s == nil || r.s.rr == nil {
		return errors.New("jobs server is not running")
	}

	for name, b := range r.s.Brokers {
		if err := b.Consume(r.s.cfg.ConsumedPipelines(name), nil, nil); err != nil {
			return err
		}
	}

	*w = "OK"
	return nil
}

// Resume all job consuming.
func (r *rpcServer) Resume(resume bool, w *string) (err error) {
	if r.s == nil || r.s.rr == nil {
		return errors.New("jobs server is not running")
	}

	for name, b := range r.s.Brokers {
		if err := b.Consume(r.s.cfg.ConsumedPipelines(name), r.s.execPool, r.s.error); err != nil {
			return err
		}
	}

	*w = "OK"
	return nil
}

// Workers returns list of active workers and their stats.
func (r *rpcServer) Workers(list bool, w *WorkerList) (err error) {
	if r.s == nil || r.s.rr == nil {
		return errors.New("jobs server is not running")
	}

	w.Workers, err = util.ServerState(r.s.rr)
	return err
}

// Stat returns list of active workers and their stats.
func (r *rpcServer) Stat(list bool, l *PipelineList) (err error) {
	if r.s == nil || r.s.rr == nil {
		return errors.New("jobs server is not running")
	}

	*l = PipelineList{}
	for name, p := range r.s.cfg.Pipelines {
		b, ok := r.s.Brokers[p.Broker()]
		if !ok {
			continue
		}

		stat, err := b.Stat(p)
		if err != nil {
			return err
		}

		stat.Name = name
		stat.Broker = p.Broker()
		l.Pipelines = append(l.Pipelines, stat)
	}

	return err
}
