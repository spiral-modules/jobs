package jobs

const (
	// EventPushComplete thrown when new job has been added. JobEvent is passed as context.
	EventPushComplete = iota + 1500

	// EventPushError caused when job can not be registered.
	EventPushError

	// EventJobComplete thrown when job execution is successfully completed. JobEvent is passed as context.
	EventJobComplete

	// EventJobError thrown on all job related errors. See JobError as context.
	EventJobError

	// EventPipelineStop when pipeline consuming has been enabled.
	EventPipelineConsume

	// EventPipelineStop when pipeline consuming has been stopped.
	EventPipelineStop

	// EventPipelineError when pipeline specific error happen.
	EventPipelineError
)

// JobEvent represent job event.
type JobEvent struct {
	// ID is job id.
	ID string

	// Job is failed job.
	Job *Job
}

// JobError represents singular Job error event.
type JobError struct {
	// ID is job id.
	ID string

	// Job is failed job.
	Job *Job

	// Caused contains job specific error.
	Caused error
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

// Caused returns error message.
func (e *PipelineError) Error() string {
	return e.Caused.Error()
}
