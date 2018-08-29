package jobs

import (
	"encoding/json"
)

// Job carries information about single job.
type Job struct {
	// ID contains unique job id.
	ID string `json:"id"`

	// Attempt is number of job attempt if case of error.
	Attempt int `json:"attempt"`

	// Job contains name of job broker (usually PHP class).
	Job string `json:"job"`

	// Pipeline associated with the job.
	Pipeline string `json:"pipeline"`

	// Payload is string data (usually JSON) passed to Job broker.
	Payload string `json:"payload"`

	// Options contains set of PipelineOptions specific to job execution. Can be empty.
	Options *Options `json:"options,omitempty"`
}

// Body packs job payload into binary payload.
func (j *Job) Body() []byte {
	return []byte(j.Payload)
}

// Context pack job context (job, id) into binary payload.
func (j *Job) Context() ([]byte, error) {
	return json.Marshal(struct {
		ID  string `json:"id"`
		Job string `json:"job"`
	}{
		ID:  j.ID,
		Job: j.Job,
	})
}

// Options carry information about how to handle given job.
type Options struct {
	// Delay defines time duration to delay execution for.
	Delay *int `json:"delay"`
}
