package jobs

// Endpoint represents single endpoint abstraction.
type Endpoint interface {
	// Push new job to the endpoint.
	Push(j *Job) error

	// Serve endpoint must listen for all associated pipelines and consume given jobs.
	Serve(svc *Service) error

	// Stop must stop endpoint.
	Stop()
}
