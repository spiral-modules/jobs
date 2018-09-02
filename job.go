package jobs

import (
	"encoding/json"
	"time"
)

// Job carries information about single job.
type Job struct {
	// Job contains name of job broker (usually PHP class).
	Job string `json:"job"`

	// Pipeline associated with the job.
	Pipeline string `json:"pipeline"`

	// Attempt is number of job attempt if case of error.
	Attempt int `json:"attempt"`

	// Payload is string data (usually JSON) passed to Job broker.
	Payload string `json:"payload"`

	// Options contains set of PipelineOptions specific to job execution. Can be empty.
	Options Options `json:"options,omitempty"`
}

// Body packs job payload into binary payload.
func (j *Job) Body() []byte {
	return []byte(j.Payload)
}

// Context pack job context (job, id) into binary payload.
func (j *Job) Context(id string) ([]byte, error) {
	return json.Marshal(struct {
		ID      string `json:"id"`
		Job     string `json:"job"`
		Attempt int    `json:"attempt"`
	}{ID: id, Job: j.Job, Attempt: j.Attempt})
}

// CanRetry must return true if broker is allowed to re-run the job.
func (j *Job) CanRetry() bool {
	return j.Attempt >= j.Options.MaxAttempts
}

// Options carry information about how to handle given job.
type Options struct {
	// Delay defines time duration to delay execution for. Defaults to none.
	Delay int `json:"delay,omitempty"`

	// RetryDelay defines for how long job should be waiting until next retry. Defaults to none.
	RetryDelay int `json:"retryDelay,omitempty"`

	// Timeout defines for how broker should wait until treating job are failed. Defaults to 30 min.
	Timeout int `json:"timeout,omitempty"`

	// Maximum job retries. Defaults to none.
	MaxAttempts int `json:"maxAttempts,omitempty"`
}

// RetryDuration returns retry delay duration in a form of time.Duration.
func (o *Options) RetryDuration() time.Duration {
	return time.Second * time.Duration(o.RetryDelay)
}

// DelayDuration returns delay duration in a form of time.Duration.
func (o *Options) DelayDuration() time.Duration {
	return time.Second * time.Duration(o.Delay)
}

// DelayDuration returns timeout duration in a form of time.Duration.
func (o *Options) TimeoutDuration() time.Duration {
	if o.Timeout == 0 {
		return 30 * time.Minute
	}

	return time.Second * time.Duration(o.Timeout)
}
