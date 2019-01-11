package amqp

import (
	"encoding/json"
	"github.com/spiral/jobs"
	"github.com/streadway/amqp"
)

type queue struct {
	// exec handlers
	execPool chan jobs.Handler
	err      jobs.ErrorHandler
}

func newQueue(pipe *jobs.Pipeline, publish, consume *chanPool) (*queue, error) {
	return nil, nil
}

// associate queue with new consume pool
func (q *queue) configure(execPool chan jobs.Handler, err jobs.ErrorHandler) error {
	q.execPool = execPool
	q.err = err

	return nil
}

func (q *queue) serve() {

}

func (q *queue) stop() {

}

func (q *queue) publish(id string, body []byte, attempt int, opts *jobs.Options) error {
	return nil
}

func (q *queue) inspect() (*amqp.Queue, error) {
	return nil, nil
}

func (q *queue) consume(delivery *amqp.Delivery) {
	j := &jobs.Job{}
	json.Unmarshal(delivery.Body, j)

	var multiple, requeue = false, false

	// requeue multiple with delay

	delivery.Nack(multiple, requeue)

	// retry
	// delivery.Ack(multiple)
	// delivery.Reject()
}
