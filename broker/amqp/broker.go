package amqp

import (
	"github.com/spiral/jobs"
)

// Broker represents AMQP broker.
type Broker struct {
	cfg  *Config
	lsns []func(event int, ctx interface{})
}

// AddListener attaches server event watcher.
func (b *Broker) AddListener(l func(event int, ctx interface{})) {
	b.lsns = append(b.lsns, l)
}

// Start configures local job broker.
func (b *Broker) Init(cfg *Config) (ok bool, err error) {
	b.cfg = cfg

	// c, err := newConn(b.cfg.Addr, "test-exchange")
	// if err != nil {
	// 	return false, err
	// }

	// q, err := c.ch.QueueDeclare(
	// 	"default", // name
	// 	true,      // type
	// 	true,      // durable
	// 	false,     // auto-deleted
	// 	false,     // internal
	// 	nil,
	// )

	////log.Println(q)
	//log.Println(err)

	return true, nil
}

// Register broker pipeline.
func (b *Broker) Register(pipe *jobs.Pipeline) error {
	return nil
}

// Serve broker pipelines.
func (b *Broker) Serve() error {

	return nil
}

// Stop all pipelines.
func (b *Broker) Stop() {

}

// Consume configures pipeline to be consumed. Set execPool to nil to disable consuming. Method can be called before
// the service is started!
func (b *Broker) Consume(pipe *jobs.Pipeline, execPool chan jobs.Handler, errHandler jobs.ErrorHandler) error {
	return nil
}

// Push job into the worker.
func (b *Broker) Push(pipe *jobs.Pipeline, j *jobs.Job) (string, error) {
	return "", nil
}

// Stat must fetch statistics about given pipeline or return error.
func (b *Broker) Stat(pipe *jobs.Pipeline) (stat *jobs.Stat, err error) {
	return nil, nil
}
