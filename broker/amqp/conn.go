package amqp

import (
	"github.com/streadway/amqp"
	"log"
	"sync"
	"time"
)

// Conn manages underling TCP connection and provides
// support for named channel association.
type Conn struct {
	addr, exchange string
	tout           time.Duration
	conn           *amqp.Connection
	mu             sync.Mutex
	channels       map[string]chanAlloc
	connErr        chan *amqp.Error

	// lock stuff?
}

type chanAlloc struct {
	ch    *amqp.Channel
	close chan error
}

func NewConn(addr string, exchange string, tout time.Duration) (*Conn, error) {
	conn, err := amqp.Dial(addr)
	if err != nil {
		return nil, err
	}

	// ch, err := conn.Channel()
	// if err != nil {
	// 	conn.Close()
	// 	return nil, err
	// }

	// // todo: hardcoded
	// err = ch.ExchangeDeclare(
	// 	exchange, // name
	// 	"direct", // type
	// 	true,     // durable
	// 	false,    // auto-deleted
	// 	false,    // internal
	// 	false,    // noWait
	// 	nil,      // arguments
	// )
	// 	if err != nil {
	// 		ch.Close()
	// 		conn.Close()
	// 		return nil, err
	// 	}

	cn := &Conn{
		addr:     addr,
		exchange: exchange,
		tout:     tout,
		conn:     conn,
		connErr:  conn.NotifyClose(make(chan *amqp.Error)),
	}

	go func() {
		for {
			select {
			case err := <-cn.connErr:
				log.Println(err)
				// todo: reconnect

				// todo: working on reconnect
				// todo: broadcast reconnect

				// todo: must lock
			}
		}
	}()

	return cn, err
}

// Channel allocates new named connection channel.
func (c *Conn) Channel(name string) (*amqp.Channel, chan error, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// todo: check if connection has been stopped

	if ca, ok := c.channels[name]; ok {
		return ca.ch, ca.close, nil
	}

	var (
		ca  = chanAlloc{}
		err error
	)

	ca.ch, err = c.conn.Channel()
	if err != nil {
		return nil, nil, err
	}

	ca.close = make(chan error)
	c.channels[name] = ca

	return ca.ch, ca.close, nil
}

// Close the channel and underlying connection.
func (c *Conn) Close() error {
	// todo:

	// if err := c.ch.Close(); err != nil {
	// c.conn.Close()
	// return err
	// }

	return c.conn.Close()
}

func m1(conn *Conn) {
	q := conn.Consume()
	for {
		select {
		case m := q:
			// err
		case <-conn.failed:
			select {
			case <-conn.restored:
				q = conn.Consume()

			case <-conn.dead:
				return
			case <-wait:

			}
		}
	}
}

func m2(conn *Conn) {
	q := conn.Consume()
	for {
		select {
		case m := q:
			// err
		case <-conn.failed:

			select {
			case <-conn.restored:
				q = conn.Consume()

			case <-conn.dead:
				return
			case <-wait:

			}

		}
	}
}
