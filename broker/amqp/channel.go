package amqp

import (
	"github.com/streadway/amqp"
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

// publish message into queue
func (c *channel) publish(id string, key string, body []byte, attempt int, headers amqp.Table) error {
	return c.ch.Publish(
		"",    // exchange
		key,   // routing key
		false, // mandatory
		false, // immediate
		amqp.Publishing{
			ContentType:  "application/octet-stream",
			Body:         body,
			DeliveryMode: amqp.Persistent,
			Headers:      headers,
		},
	)
}

// gracefully stop channel
func (c *channel) Close() error {
	c.signal <- nil
	return c.ch.Close()
}
