package jobs

import (
	"strings"
)

// Dispatcher provides ability to automatically locate the pipeline for the specific job
// and update job options (if none set).
type Dispatcher map[string]Options

// clarify clarifies target job pipeline and other job options. Can return nil.
func (d Dispatcher) clarify(job *Job) *Options {
	var (
		found Options
		best  = 0
	)

	for pattern, opts := range d {
		pattern = strings.Replace(strings.Trim(pattern, "-.*"), "-", ".", -1)

		if strings.Contains(job.Job, pattern) && len(pattern) > best {
			found = opts
			best = len(pattern)
		}
	}

	if best == 0 {
		return nil
	}

	return &found
}
