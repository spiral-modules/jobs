package amqp

import "github.com/streadway/amqp"

type Conn struct {
	conn *amqp.Connection
	ch   *amqp.Channel
}

func newConn(addr string, exchange string) (*Conn, error) {
	conn, err := amqp.Dial(addr)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchange, // name
		"direct", // type
		true,     // durable
		false,    // auto-deleted
		false,    // internal
		false,    // noWait
		nil,      // arguments
	)

	if err != nil {
		ch.Close()
		conn.Close()
		return nil, err
	}

	return &Conn{conn: conn, ch: ch}, err
}

// Close the channel and underlying connection.
func (c *Conn) Close() error {
	if err := c.ch.Close(); err != nil {
		c.conn.Close()
		return err
	}

	return c.conn.Close()
}
