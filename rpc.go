package jobs

import (
	"errors"
	"fmt"
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

// Destroy job consuming for a given pipeline.
func (r *rpcServer) Stop(pipeline string, w *string) (err error) {
	if r.s == nil || r.s.rr == nil {
		return errors.New("jobs server is not running")
	}

	pipe := r.s.Pipelines().Get(pipeline)
	if pipe == nil {
		return fmt.Errorf("undefined pipeline `%s`", pipeline)
	}

	if err := r.s.Consume(pipe, nil, nil); err != nil {
		return err
	}

	*w = "OK"
	return nil
}

// Resume job consuming for a given pipeline.
func (r *rpcServer) Resume(pipeline string, w *string) (err error) {
	if r.s == nil || r.s.rr == nil {
		return errors.New("jobs server is not running")
	}

	pipe := r.s.Pipelines().Get(pipeline)
	if pipe == nil {
		return fmt.Errorf("undefined pipeline `%s`", pipeline)
	}

	if err := r.s.Consume(pipe, r.s.execPool, r.s.error); err != nil {
		return err
	}

	*w = "OK"
	return nil
}

// Destroy job consuming for a given pipeline.
func (r *rpcServer) StopAll(stop bool, w *string) (err error) {
	if r.s == nil || r.s.rr == nil {
		return errors.New("jobs server is not running")
	}

	for _, pipe := range r.s.Pipelines() {
		if err := r.s.Consume(pipe, nil, nil); err != nil {
			return err
		}
	}

	*w = "OK"
	return nil
}

// Resume job consuming for a given pipeline.
func (r *rpcServer) ResumeAll(resume bool, w *string) (err error) {
	if r.s == nil || r.s.rr == nil {
		return errors.New("jobs server is not running")
	}

	for _, pipe := range r.s.Pipelines() {
		if err := r.s.Consume(pipe, r.s.execPool, r.s.error); err != nil {
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
	for _, p := range r.s.Pipelines() {
		stat, err := r.s.Stat(p)
		if err != nil {
			return err
		}

		l.Pipelines = append(l.Pipelines, stat)
	}

	return err
}
