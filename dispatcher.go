package jobs

import (
	"strings"
)

// Dispatcher provides ability to automatically locate the pipeline for the specific job
// and update job options (if none set).
type Dispatcher map[string]*Options

// match clarifies target job pipeline and other job options. Can return nil.
func (d Dispatcher) match(job *Job) (found *Options) {
	var best = 0

	for pattern, opts := range d {
		pattern = d.prepare(pattern)
		if strings.HasPrefix(job.Job, pattern) && len(pattern) > best {
			found = opts
			best = len(pattern)
		}
	}

	if best == 0 {
		return nil
	}

	return found
}

// prepare pattern for comparision
func (d *Dispatcher) prepare(pattern string) string {
	return strings.Replace(strings.Trim(pattern, "-.*"), "-", ".", -1)
}
