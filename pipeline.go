package jobs

import (
	"strings"
	"time"
)

// Pipeline describes broker specific pipeline.
type Pipeline struct {
	// Broker defines name of associated broker.
	Broker string

	// Listen define job matching pattern (i.e. - "app.jobs.email")
	Handle []string

	// Retry defined number of job retries in case of error. Default none.
	Retry int

	// RetryDelay defines for how long wait till job retry.
	RetryDelay int

	// Listen tells the service that this pipeline must be consumed by the service.
	Listen bool

	// Options are broker specific PipelineOptions.
	Options PipelineOptions
}

// Listen must return true if pipeline expect to handle given job.
func (p *Pipeline) Has(job string) bool {
	for _, j := range p.Handle {
		if strings.Contains(job, strings.Trim(j, ".*")) {
			return true
		}
	}

	return false
}

type PipelineOptions map[string]interface{}

// Bool must return option value as string or return default value.
func (o PipelineOptions) Bool(name string, d bool) bool {
	if value, ok := o[name]; ok {
		if b, ok := value.(bool); ok {
			return b
		}
	}

	return d
}

// String must return option value as string or return default value.
func (o PipelineOptions) String(name string, d string) string {
	if value, ok := o[name]; ok {
		if str, ok := value.(string); ok {
			return str
		}
	}

	return d
}

// Int must return option value as string or return default value.
func (o PipelineOptions) Integer(name string, d int) int {
	if value, ok := o[name]; ok {
		if str, ok := value.(int); ok {
			return str
		}
	}

	return d
}

// Duration must return option value as time.Duration (seconds) or return default value.
func (o PipelineOptions) Duration(name string, d time.Duration) time.Duration {
	if value, ok := o[name]; ok {
		if str, ok := value.(int); ok {
			return time.Second * time.Duration(str)
		}
	}

	return d
}
