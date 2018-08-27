package jobs

// Endpoint represents single endpoint abstraction.
type Endpoint interface {
	// Handler must be provided by parent service and define how to execute job.
	Handler(h Handler)

	// Push new job to the endpoint.
	Push(j *Job) error

	// Serve endpoint must listen for all associated pipelines and consume given jobs.
	Serve() error

	// Stop must stop endpoint.
	Stop()
}
