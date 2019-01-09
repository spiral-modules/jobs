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
