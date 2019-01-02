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
	Stat(p *Pipeline) (stat *Stat, err error)
}

// Stat contains information about pipeline job numbers.
type Stat struct {
	// Broken is name of associated broker.
	Broker string

	// Pipeline name.
	Pipeline string

	// Queue defines number of pending jobs.
	Queue int64

	// Active defines number of jobs which are currently being processed.
	Active int64

	// Delayed defines number of jobs which are being processed.
	Delayed int64
}

// todo: freeze
