package jobs

// Endpoint represents single endpoint abstraction.
type Endpoint interface {
	// Handle must be provided by parent service and define how to execute job.
	Handle(pipes []*Pipeline, h Handler) Endpoint

	// Push new job to the endpoint.
	Push(p *Pipeline, j *Job) error

	// Serve endpoint must listen for all associated pipelines and consume given jobs.
	Serve() error

	// Stop must stop endpoint.
	Stop()
}
