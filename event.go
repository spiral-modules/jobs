package jobs

import "time"

const (
	// EventPushComplete thrown when new job has been added. JobEvent is passed as context.
	EventPushComplete = iota + 1500

	// EventPushError caused when job can not be registered.
	EventPushError

	// EventJobReceived thrown when new job received.
	EventJobReceived

	// EventJobComplete thrown when job execution is successfully completed. JobEvent is passed as context.
	EventJobComplete

	// EventJobError thrown on all job related errors. See JobError as context.
	EventJobError

	// EventPipelineConsume when pipeline consuming has been requested.
	EventPipelineConsume

	// EventPipelineConsuming when pipeline consuming has started.
	EventPipelineConsuming

	// EventPipelineStop when pipeline consuming has begun stopping.
	EventPipelineStop

	// EventPipelineStopped when pipeline consuming has been stopped.
	EventPipelineStopped

	// EventPipelineError when pipeline specific error happen.
	EventPipelineError

	// EventBrokerReady thrown when broken is ready to accept/serve tasks.
	EventBrokerReady
)

// JobEvent represent job event.
type JobEvent struct {
	// String is job id.
	ID string

	// Job is failed job.
	Job *Job

	// event timings
	start   time.Time
	elapsed time.Duration
}

// Elapsed returns duration of the invocation.
func (e *JobEvent) Elapsed() time.Duration {
	return e.elapsed
}

// JobError represents singular Job error event.
type JobError struct {
	// String is job id.
	ID string

	// Job is failed job.
	Job *Job

	// Caused contains job specific error.
	Caused error

	// event timings
	start   time.Time
	elapsed time.Duration
}

// Elapsed returns duration of the invocation.
func (e *JobError) Elapsed() time.Duration {
	return e.elapsed
}

// Caused returns error message.
func (e *JobError) Error() string {
	return e.Caused.Error()
}

// PipelineError defines pipeline specific errors.
type PipelineError struct {
	// Pipeline is associated pipeline.
	Pipeline *Pipeline

	// Caused send by broker.
	Caused error
}

// Error returns error message.
func (e *PipelineError) Error() string {
	return e.Caused.Error()
}
