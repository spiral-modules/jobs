package jobs

type Broker interface {
	// Listen configures broker with list of pipelines to listen and handler function.
	Register(pipes []*Pipeline) error

	// Consume configures pipelines to be consumes. Set execPool to nil to disable consuming.
	Consume(pipes []*Pipeline, execPool chan Handler, err ErrorHandler) error

	// Push job into the worker.
	Push(pipe *Pipeline, j *Job) (string, error)

	// Stat must fetch statistics about given pipeline or return error.
	Stat(pipe *Pipeline) (stat *Stat, err error)
}

// Stat contains information about pipeline.
type Stat struct {
	// Name is pipeline public name.
	Name string

	// Broken is name of associated broker.
	Broker string

	// Consume indicates that pipeline is consuming jobs.
	Consume bool

	// Pipeline name.
	Pipeline string

	// Queue defines number of pending jobs.
	Queue int64

	// Active defines number of jobs which are currently being processed.
	Active int64

	// Delayed defines number of jobs which are being processed.
	Delayed int64
}
