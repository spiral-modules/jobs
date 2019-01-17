package amqp

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
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
	// todo: more conn options
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
	log.Printf(">>> NEW    ------ %p\n", cp)

	return cp, nil
}

// Close gracefully closes all underlying channels and connection.
func (cp *chanPool) Close() error {
	log.Printf(">>> CLOSING    ------ %p\n", cp)
	defer log.Printf(">>> CLOSED    ------ %p\n", cp)

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
			cp.closeChan(ch, nil)
		}(ch)
	}
	cp.mu.Unlock()

	wg.Wait()

	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.conn != nil {
		return cp.conn.Close()
	}

	return nil
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
			log.Printf("GOT ERR %p: %s\n", cp, err)
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

			conn, errChan := cp.reconnect(addr)
			if conn == nil {
				log.Printf(">>> DROPPED IN ERROR ------ %p\n", cp)
				// what is that?
				cp.mu.Lock()
				close(cp.connected)
				cp.mu.Unlock()

				// interrupted
				return
			}

			cp.mu.Lock()
			cp.conn = conn
			cp.channels = make(map[string]*channel)
			errors = errChan
			cp.mu.Unlock()

			close(cp.connected)
		}
	}
}

func (cp *chanPool) reconnect(addr string) (conn *amqp.Connection, errors chan *amqp.Error) {
	for {
		select {
		case <-cp.wait:
			// connection has been cancelled is not possible
			return nil, nil

		case <-time.NewTimer(cp.tout).C:
			// todo: more conn options
			conn, err := amqp.Dial(addr)
			if err != nil {
				// still failing
				continue
			}

			return conn, conn.NotifyClose(make(chan *amqp.Error))
		}
	}
}

// channel allocates new channel on amqp connection
func (cp *chanPool) channel(name string) (*channel, error) {
	cp.mu.Lock()
	dead := cp.conn == nil
	cp.mu.Unlock()

	if dead {
		// wait for connection restoration (doubled the timeout duration)
		select {
		case <-time.NewTimer(cp.tout * 2).C:
			return nil, fmt.Errorf("connection is dead")
		case <-cp.connected:
			// connected
		}
	}

	cp.mu.Lock()
	defer cp.mu.Unlock()

	if cp.conn == nil {
		return nil, fmt.Errorf("connection has been closed")
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

// closeChan gracefully closes and removes channel allocation.
func (cp *chanPool) closeChan(c *channel, err error) error {
	cp.mu.Lock()
	defer cp.mu.Unlock()

	log.Println("close cnan cos", err)

	go func() {
		c.signal <- nil
		c.ch.Close()
	}()

	for name, ch := range cp.channels {
		if ch == c {
			delete(cp.channels, name)
		}
	}

	return err
}
