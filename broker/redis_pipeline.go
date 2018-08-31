package broker

import (
	"github.com/spiral/jobs"
	"fmt"
)

// redis specific pipeline configuration
type pipeline struct {
	// Listen the pipeline.
	Listen bool

	// Queue name.
	Queue string

	// Mode defines operating mode (fifo, lifo, broadcast)
	Mode string

	// Timeout defines listen timeout, defaults to 1.
	Timeout int
}

func makePipeline(p *jobs.Pipeline) (*pipeline, error) {
	rp := &pipeline{
		Listen:  p.Listen,
		Queue:   p.Options.String("queue", ""),
		Mode:    p.Options.String("mode", "fifo"),
		Timeout: p.Options.Integer("timeout", 1),
	}

	if err := rp.Valid(); err != nil {
		return nil, err
	}

	return rp, nil
}

// Valid returns error if pipeline configuration is not valid.
func (p *pipeline) Valid() error {
	if p.Queue == "" {
		return fmt.Errorf("missing `queue` option for redis pipeline")
	}

	if p.Mode != "fifo" && p.Mode != "lifo" {
		return fmt.Errorf("undefined pipeline mode `%s` [fifo|lifo]", p.Mode)
	}

	if p.Timeout < 0 {
		return fmt.Errorf("invalid pipeline timeout %v", p.Timeout)
	}

	return nil
}
