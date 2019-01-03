package jobs

import (
	"strings"
)

// Mapper provides ability to automatically locate the pipeline for the specific job
type Mapper map[string][]string

// find target pipeline
func (m Mapper) find(job string) string {
	var (
		found = ""
		best  = 0
	)

	for target, patterns := range m {
		for _, p := range patterns {
			if strings.Contains(job, strings.Trim(p, ".*")) {
				if len(p) > best {
					found = target
					best = len(p)
				}
			}
		}
	}

	return found
}
