package jobs

const (
	// EventPushComplete thrown when new job has been added. JobEvent is passed as context.
	EventPushComplete = iota + 1500

	// EventPushError caused when job can not be registered.
	EventPushError

	// EventJobComplete thrown when job execution is successfully completed. JobEvent is passed as context.
	EventJobComplete

	// EventJobError thrown on all job related errors. See ErrorEvent as context.
	EventJobError

	// EventBrokerError defines broker specific error.
	EventBrokerError

	// EventPipelineStop when pipeline consuming has been enabled.
	EventPipelineConsume

	// EventPipelineStop when pipeline consuming has been stopped.
	EventPipelineStop
)

// JobEvent represent job event.
type JobEvent struct {
	// ID is job id.
	ID string

	// Job is failed job.
	Job *Job
}

// ErrorEvent represents singular Job error event.
type ErrorEvent struct {
	// ID is job id.
	ID string

	// Job is failed job.
	Job *Job

	// Error - associated error, if any.
	Error error
}
