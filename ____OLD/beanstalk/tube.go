package beanstalk

import (
	"errors"
	"github.com/beanstalkd/go-beanstalk"
	"github.com/spiral/jobs"
	"strconv"
	"sync"
)

// Tube connects with singular queue channel in beanstalk.
type Tube struct {
	*beanstalk.Tube

	// locks tube
	mu sync.Mutex

	// Indicates that tube must be listened.
	Listen bool
}

// NewTube creates new tube or returns an error
func NewTube(p *jobs.Pipeline) (*Tube, error) {
	if p.Options.String("tube", "") == "" {
		return nil, errors.New("missing `tube` parameter on beanstalk pipeline")
	}

	return &Tube{Tube: &beanstalk.Tube{Name: p.Options.String("tube", "")}, Listen: p.Listen}, nil
}

// Stats fetches and formats tube stats.
func (t *Tube) fetchStats() (stat *jobs.Stat, err error) {
	values, err := t.Stats()

	if err != nil {
		return nil, err
	}

	stat = &jobs.Stat{Pipeline: t.Name}

	if v, err := strconv.Atoi(values["current-jobs-ready"]); err == nil {
		stat.Queue = int64(v)
	}

	if v, err := strconv.Atoi(values["current-jobs-reserved"]); err == nil {
		stat.Active = int64(v)
	}

	if v, err := strconv.Atoi(values["current-jobs-delayed"]); err == nil {
		stat.Delayed = int64(v)
	}

	return stat, nil
}
