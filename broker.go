package jobs

// Broker represents single broker abstraction.
type Broker interface {
	// Handle configures broker with list of pipelines to listen and handler function.
	Handle(pipelines []*Pipeline, h Handler) error

	// Push new job to the broker.
	Push(p *Pipeline, j *Job) error

	// Serve broker must listen for all associated pipelines and consume given jobs.
	Serve() error

	// Stop must stop broker.
	Stop()
}
