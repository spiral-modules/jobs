package amqp

import (
	"fmt"
	"github.com/spiral/jobs"
	"github.com/streadway/amqp"
	"time"
)

type channel struct {
	ch       *amqp.Channel
	consumer string
	signal   chan error
	confirm  chan amqp.Confirmation
}

func newChannel(ch *amqp.Channel) *channel {
	ch.NotifyPublish(make(chan amqp.Confirmation))

	// we expect that every allocated channel would have listener on signal
	// this is not true only in case of pure producing channels
	return &channel{
		ch:      ch,
		signal:  make(chan error, 1),
		confirm: ch.NotifyPublish(make(chan amqp.Confirmation)),
	}
}

func (c *channel) exchangeDeclare(exchange string) error {
	return c.ch.ExchangeDeclare(
		exchange, // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // noWait
		nil,      // arguments
	)
}

func (c *channel) queueDeclare(queue string) error {
	// todo: more options
	_, err := c.ch.QueueDeclare(
		queue, // name
		true,  // type
		false, // durable
		false, // auto-deleted
		false, // internal
		nil,
	)

	return err
}

// bind queue (todo: better options)
func (c *channel) queueBind(queue, routingKey, exchange string) error {
	return c.ch.QueueBind(
		queue,
		routingKey,
		exchange,
		false,
		nil,
	)
}

func (c *channel) publish(id, routingKey string, job *jobs.Job) error {
	// todo: watch for publish delivery

	err := c.ch.Publish(
		"",        // exchange
		"default", // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         []byte(time.Now().String()),
			DeliveryMode: amqp.Persistent,
		},
	)

	if err != nil {
		return 0, err
	}

	confirm := <-c.confirm

	if !confirm.Ack {
		return 0, fmt.Errorf("unable to publish message into queue `%s`", queue)
	}

	return confirm.DeliveryTag, nil
}

func (c *channel) publishDelay(queue string) (uint64, error) {
	// todo: watch for publish delivery

	err := c.ch.Publish(
		"",        // exchange
		"default", // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType:  "text/plain",
			Body:         []byte(time.Now().String()),
			DeliveryMode: amqp.Persistent,
		},
	)

	if err != nil {
		return 0, err
	}

	confirm := <-c.confirm

	if !confirm.Ack {
		return 0, fmt.Errorf("unable to publish message into queue `%s`", queue)
	}

	return confirm.DeliveryTag, nil
}

func (c *channel) consume() (<-chan amqp.Delivery, chan error, error) {

}

// close channel (automatically stops consuming).
func (c *channel) close() {
	c.signal <- nil
}
