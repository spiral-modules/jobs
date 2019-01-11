package amqp

import (
	"errors"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

type chanPool struct {
	tout     time.Duration
	mu       sync.Mutex
	conn     *amqp.Connection
	channels map[string]*channel
	wait     chan interface{}
	restored chan interface{}
}

// newConn creates new watched connection
func newConn(addr string, tout time.Duration) (*chanPool, error) {
	conn, err := amqp.Dial(addr)
	if err != nil {
		return nil, err
	}

	cp := &chanPool{
		tout:     tout,
		conn:     conn,
		channels: make(map[string]*channel),
		wait:     make(chan interface{}),
	}

	go cp.watch(addr, conn.NotifyClose(make(chan *amqp.Error)))

	return cp, nil
}

// Close gracefully closes all underlying channels and connection.
func (cp *chanPool) Close() error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	close(cp.wait)
	if cp.channels == nil {
		return errors.New("connection is dead")
	}

	// close all channels and consumers
	var wg sync.WaitGroup
	for _, ch := range cp.channels {
		wg.Add(1)

		go func(ch *channel) {
			defer wg.Done()
			cp.closeChannel(ch)
		}(ch)
	}

	wg.Wait()
	return cp.conn.Close()
}

// watch manages connection state and reconnects if needed
func (cp *chanPool) watch(addr string, errors chan *amqp.Error) {
	for {
		select {
		case <-cp.wait:
			// connection has been closed
			return
		case err := <-errors:
			cp.mu.Lock()
			cp.restored = make(chan interface{})

			// broadcast error to all consumers to let them for the tryReconnect
			for _, ch := range cp.channels {
				ch.signal <- err
			}

			// disable channel allocation while server is dead
			cp.conn = nil
			cp.channels = nil
			cp.mu.Unlock()

			// reconnect loop
			for {
				select {
				case <-cp.wait:
					// restored is not possible
					close(cp.restored)
					return

				case <-time.NewTimer(cp.tout).C:
					// todo: need better dial method
					conn, err := amqp.Dial(addr)

					if err != nil {
						// still failing
						continue
					}

					cp.mu.Lock()

					cp.conn = conn
					cp.channels = make(map[string]*channel)

					// return to normal watch state
					errors = conn.NotifyClose(make(chan *amqp.Error))
					cp.mu.Unlock()

					// ready to move on and let all subscribers know that conn is restored
					close(cp.restored)
					cp.restored = nil

					break
				}
			}
		}
	}
}

// reconnected waits till connection is connected again or eventually closed.
// must only be invoked after connection error has been delivered to channel.signal.
func (cp *chanPool) reconnect() chan interface{} {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	return cp.restored
}

// channel allocates new channel on amqp connection
func (cp *chanPool) channel(name string) (*channel, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.channels == nil {
		return nil, errors.New("connection is dead")
	}

	if ch, ok := cp.channels[name]; ok {
		return ch, nil
	}

	// we must create new channel
	ch, err := cp.conn.Channel()
	if err != nil {
		return nil, err
	}

	// we expect that every allocated channel would have listener on signal
	// this is not true only in case of pure producing channels
	cp.channels[name] = newChannel(ch)

	return cp.channels[name], nil
}

// closeChannel gracefully closes and removes channel allocation.
func (cp *chanPool) closeChannel(c *channel) {
	c.close()

	cp.mu.Lock()
	for name, ch := range cp.channels {
		if ch == c {
			delete(cp.channels, name)
		}
	}
	cp.mu.Unlock()
}
