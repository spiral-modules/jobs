package jobs

// Endpoint represents single endpoint abstraction.
type Endpoint interface {
	// Handle configures endpoint with list of pipelines to listen and handler function.
	Handle(pipelines []*Pipeline, h Handler) error

	// Push new job to the endpoint.
	Push(p *Pipeline, j *Job) error

	// Serve endpoint must listen for all associated pipelines and consume given jobs.
	Serve() error

	// Stop must stop endpoint.
	Stop()
}
