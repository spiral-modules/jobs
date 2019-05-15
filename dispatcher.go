package jobs

import (
	"strings"
)

var separators = []string{"/", "-", "\\"}

// Dispatcher provides ability to automatically locate the pipeline for the specific job
// and update job options (if none set).
type Dispatcher map[string]*Options

// pre-compile patterns
func NewDispatcher(routes map[string]*Options) (d Dispatcher) {
	for pattern, opts := range routes {
		pattern = strings.ToLower(pattern)
		pattern = strings.Trim(pattern, "-.*")

		for _, s := range separators {
			pattern = strings.Replace(pattern, s, ".", -1)
		}

		d[d.prepare(pattern)] = opts
	}

	return d
}

// match clarifies target job pipeline and other job options. Can return nil.
func (d Dispatcher) match(job *Job) (found *Options) {
	var best = 0

	jobName := strings.ToLower(job.Job)
	for pattern, opts := range d {
		if strings.HasPrefix(jobName, pattern) && len(pattern) > best {
			found = opts
			best = len(pattern)
		}
	}

	if best == 0 {
		return nil
	}

	return found
}
