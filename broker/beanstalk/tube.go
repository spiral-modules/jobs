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
}

// NewTube creates new tube or returns an error
func NewTube(p *jobs.Pipeline) (*Tube, error) {
	if p.Options.String("tube", "") == "" {
		return nil, errors.New("missing `tube` parameter on beanstalk pipeline")
	}

	return &Tube{Tube: &beanstalk.Tube{Name: p.Options.String("tube", "")}, Listen: p.Listen}, nil
}
