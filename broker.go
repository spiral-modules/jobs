package jobs

// Broker manages set of pipelines and provides ability to push jobs into them.
type Broker interface {
	// Register broker specific pipelines.
	Register(pipes []*Pipeline) error

	// Consume configures pipeline to be consumed. Set execPool to nil to disable consuming. Method can be called before
	// the service is started!
	Consume(pipe *Pipeline, execPool chan Handler, errHandler ErrorHandler) error

	// Push job into the worker.
	Push(pipe *Pipeline, j *Job) (string, error)

	// Stat must fetch statistics about given pipeline or return error.
	Stat(pipe *Pipeline) (stat *Stat, err error)
}

// EventProvider defines the ability to throw events for the broker.
type EventProvider interface {
	// AddListener attaches the even listener.
	AddListener(l func(event int, ctx interface{}))
}

// Stat contains information about pipeline.
type Stat struct {
	// Pipeline name.
	Pipeline string

	// Broken is name of associated broker.
	Broker string

	// InternalName defines internal broker specific pipeline name.
	InternalName string

	// Consuming indicates that pipeline is consuming jobs.
	Consuming bool

	// queue defines number of pending jobs.
	Queue int64

	// Active defines number of jobs which are currently being processed.
	Active int64

	// Delayed defines number of jobs which are being processed.
	Delayed int64
}
