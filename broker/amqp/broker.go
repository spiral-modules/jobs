package amqp

import (
	"github.com/spiral/jobs"
	"sync"
)

// Broker represents AMQP broker.
type Broker struct {
	cfg        *Config
	lsns       []func(event int, ctx interface{})
	mu         sync.Mutex
	wait       chan error
	sharedConn *Conn
}

// AddListener attaches server event watcher.
func (b *Broker) AddListener(l func(event int, ctx interface{})) {
	b.lsns = append(b.lsns, l)
}

// Start configures local job broker.
func (b *Broker) Init(cfg *Config) (ok bool, err error) {
	b.cfg = cfg

	conn, err := NewConn(b.cfg.Addr, b.cfg.Exchange)
	if err != nil {
		return false, err
	}
	b.sharedConn = conn

	// c, err := NewConn(b.cfg.Addr, "test-exchange")
	// if err != nil {
	// 	return false, err
	// }
	//
	// close := make(chan *amqp.Error, 1)
	// c.conn.NotifyClose(close)
	//
	// go func (){
	// 	<-close
	// 	log.Println("DEAD CONNECTION")
	// }()
	//
	// q, err := c.ch.QueueDeclare(
	// 	"default", // name
	// 	true,      // type
	// 	false,     // durable
	// 	false,     // auto-deleted
	// 	false,     // internal
	// 	nil,
	// )
	//
	// log.Println(q.Name)
	//
	// go func() {
	// 	for i := 0; i < 1000; i++ {
	// 		err = c.ch.Publish(
	// 			"",     // exchange
	// 			q.Name, // routing key
	// 			false,  // mandatory
	// 			false,  // immediate
	// 			amqp.Publishing{
	// 				ContentType:  "text/plain",
	// 				Body:         []byte(time.Now().String()),
	// 				DeliveryMode: amqp.Persistent,
	// 			},
	// 		)
	//
	// 		time.Sleep(time.Second)
	// 	}
	// }()
	//
	// msgs, err := c.ch.Consume(
	// 	q.Name, // queue
	// 	"",     // consumer
	// 	false,  // auto-ack
	// 	false,  // exclusive
	// 	false,  // no-local
	// 	false,  // no-wait
	// 	nil,    // args
	// )
	//
	// go func() {
	// 	for d := range msgs {
	// 		log.Printf("Received a message: %s", d.Body)
	// 		d.Ack(false)
	// 	}
	// }()

	// //log.Println(q)
	// log.Println(err)

	return true, nil
}

// Register broker pipeline.
func (b *Broker) Register(pipe *jobs.Pipeline) error {
	return nil
}

// Serve broker pipelines.
func (b *Broker) Serve() error {
	b.mu.Lock()

	// todo: start queues

	b.wait = make(chan error)
	b.mu.Unlock()

	return <-b.wait
}

// Stop all pipelines.
func (b *Broker) Stop() {
	b.wait <- nil
	b.sharedConn.Close()
}

// Consume configures pipeline to be consumed. With execPool to nil to disable consuming. Method can be called before
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
