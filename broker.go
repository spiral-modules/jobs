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
	Name      string
	Details   string
	Total     int64
	Pending   int64
	Active    int64
	Delayed   int64
	Failed    int64
	Completed int64
}
