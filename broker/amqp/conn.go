package amqp

import (
	"fmt"
	"github.com/streadway/amqp"
	"sync"
	"time"
)

// manages set of AMQP channels
type chanPool struct {
	tout      time.Duration
	mu        sync.Mutex
	conn      *amqp.Connection
	channels  map[string]*channel
	wait      chan interface{}
	connected chan interface{}
}

// manages single channel
type channel struct {
	ch       *amqp.Channel
	consumer string
	signal   chan error
}

// newConn creates new watched AMQP connection
func newConn(addr string, tout time.Duration) (*chanPool, error) {
	conn, err := amqp.Dial(addr)
	if err != nil {
		return nil, err
	}

	cp := &chanPool{
		tout:      tout,
		conn:      conn,
		channels:  make(map[string]*channel),
		wait:      make(chan interface{}),
		connected: make(chan interface{}),
	}

	close(cp.connected)
	go cp.watch(addr, conn.NotifyClose(make(chan *amqp.Error)))

	return cp, nil
}

// Close gracefully closes all underlying channels and connection.
func (cp *chanPool) Close() error {
	cp.mu.Lock()

	close(cp.wait)
	if cp.channels == nil {
		return fmt.Errorf("connection is dead")
	}

	// close all channels and consume
	var wg sync.WaitGroup
	for _, ch := range cp.channels {
		wg.Add(1)

		go func(ch *channel) {
			defer wg.Done()
			cp.release(ch, nil)
		}(ch)
	}
	cp.mu.Unlock()

	wg.Wait()
	return cp.conn.Close()
}

// waitConnected waits till connection is connected again or eventually closed.
// must only be invoked after connection error has been delivered to channel.signal.
func (cp *chanPool) waitConnected() chan interface{} {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	return cp.connected
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
			cp.connected = make(chan interface{})

			// broadcast error to all consume to let them for the tryReconnect
			for _, ch := range cp.channels {
				ch.signal <- err
			}

			// disable channel allocation while server is dead
			cp.conn = nil
			cp.channels = nil
			cp.mu.Unlock()

			// waitConnected loop
		reconnecting:
			for {
				select {
				case <-cp.wait:
					// connection has been cancelled is not possible
					close(cp.connected)
					return

				case <-time.NewTimer(cp.tout).C:

					// todo: need better dial method (TSL and etc)
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

					close(cp.connected)
					cp.mu.Unlock()

					break reconnecting
				}
			}
		}
	}
}

// channel allocates new channel on amqp connection
func (cp *chanPool) channel(name string) (*channel, error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.channels == nil {
		return nil, fmt.Errorf("connection is dead")
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
	cp.channels[name] = &channel{ch: ch, signal: make(chan error, 1)}

	return cp.channels[name], nil
}

// release gracefully closes and removes channel allocation.
func (cp *chanPool) release(c *channel, err error) {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	go func() {
		c.signal <- nil
		c.ch.Close()
	}()

	for name, ch := range cp.channels {
		if ch == c {
			delete(cp.channels, name)
		}
	}
}
