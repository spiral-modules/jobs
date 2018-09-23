package beanstalk

import (
	"errors"
	"github.com/spiral/jobs"
	"github.com/xuri/aurora/beanstalk"
)

// Tube connects with singular queue channel in beanstalk.
type Tube struct {
	*beanstalk.Tube

	// Indicates that tube must be listened.
	Listen bool

	// Number of threads to serve tube with.
	Threads int
}

// NewTube creates new tube or returns an error
func NewTube(p *jobs.Pipeline) (*Tube, error) {
	if p.Options.String("tube", "") == "" {
		return nil, errors.New("missing `tube` parameter on beanstalk pipeline")
	}

	if p.Options.Integer("threads", 1) < 1 {
		return nil, errors.New("invalid `threads` value for beanstalk pipeline, must be 1 or greater")
	}

	return &Tube{
		Tube:    &beanstalk.Tube{Name: p.Options.String("tube", "")},
		Listen:  p.Listen,
		Threads: p.Options.Integer("threads", 1),
	}, nil
}
