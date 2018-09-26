package jobs

// Broker represents single broker abstraction.
type Broker interface {
	// Listen configures broker with list of pipelines to listen and handler function.
	Listen(pipelines []*Pipeline, pool chan Handler, err ErrorHandler) error

	// Serve broker must listen for all associated pipelines and consume given jobs.
	Serve() error

	// Stop must stop broker.
	Stop()

	// Push new job to the broker. Must return job id or error.
	Push(p *Pipeline, j *Job) (id string, err error)

	// Stat must fetch statistics about given pipeline or return error.
	Stat(p *Pipeline) (stats *PipelineStat, err error)
}

type PipelineStat struct {
	Name          string
	Details       string
	JobsTotal     int
	JobsDelayed   int
	JobsErrored   int
	JobsCompleted int
}
