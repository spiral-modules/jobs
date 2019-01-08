package beanstalk

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// connPool manages set of connections with ability to retry operation using another connection in case of pipe error.
type connPool struct {
	// pool options
	size      int
	reconnect time.Duration
	new       func() (io.Closer, error)

	// active connections
	muc   sync.Mutex
	conns []interface{}

	// disable refilling
	inDestroy int32

	// active allocations
	muw sync.Mutex
	wg  sync.WaitGroup

	// free connections
	free chan interface{}
	dead chan interface{}

	// stop the pool
	wait chan interface{}
}

// Start creates given number of pending connections. Non thread safe.
func (p *connPool) Start() error {
	p.wait = make(chan interface{})
	p.conns = make([]interface{}, 0, p.size)

	p.free = make(chan interface{}, p.size)
	p.dead = make(chan interface{}, p.size)

	// connect in parallel
	start := make(chan interface{}, p.size)
	for i := 0; i < p.size; i++ {
		go func() {
			c, err := p.createConn()
			if err != nil {
				start <- err
			}

			p.free <- c
			start <- nil
		}()
	}

	for i := 0; i < p.size; i++ {
		r := <-start
		if err, ok := r.(error); ok {
			p.Destroy()
			return err
		}
	}

	go p.serve()

	return nil
}

// Allocate free connection or return error. Blocked until connection is available or pool is dead.
func (p *connPool) Allocate(tout time.Duration) (interface{}, error) {
	p.muw.Lock()
	defer p.muw.Unlock()

	select {
	case <-p.wait:
		return nil, fmt.Errorf("unable to allocate connection (pool closed)")
	case c := <-p.free:
		p.wg.Add(1)
		return c, nil
	default:
		// try again with timeout
		timeout := time.NewTimer(tout)
		select {
		case <-p.wait:
			timeout.Stop()
			return nil, fmt.Errorf("unable to allocate connection (pool closed)")
		case c := <-p.free:
			timeout.Stop()
			p.wg.Add(1)
			return c, nil
		case <-timeout.C:
			return nil, fmt.Errorf("connection allocate timeout (%s)", tout)
		}
	}
}

// Release the connection with or without context error. If error is instance of connError then
// connection will be recreated.
func (p *connPool) Release(conn interface{}, err error) {
	p.wg.Done()

	if _, ok := err.(connError); ok {
		// connection issue, retry?
		p.replaceConn(conn)
		return
	}

	p.free <- conn
}

// Destroy pool and close all underlying connections. Thread safe.
func (p *connPool) Destroy() {
	if atomic.LoadInt32(&p.inDestroy) == 1 {
		return
	}

	atomic.AddInt32(&p.inDestroy, 1)

	close(p.wait)
	p.muw.Lock()
	p.wg.Wait() // wait for all connection to be released
	p.muw.Unlock()
	close(p.free)

	p.muc.Lock()
	defer p.muc.Unlock()
	for _, c := range p.conns {
		if closer, ok := c.(io.Closer); ok {
			closer.Close()
		}
	}

	p.conns = nil
}

// serve connections and dead dead sockets.
func (p *connPool) serve() {
	var (
		retry    = time.NewTicker(p.reconnect)
		deadConn = 0
	)

	for {
		select {
		case <-p.wait:
			retry.Stop()
			return
		case <-retry.C:
			if deadConn == 0 {
				// nothing to retry
				continue
			}

			if !p.acquireLock() {
				// the pool is dead, chimichanga
				return
			}

			// try to make one test connection
			conn, err := p.createConn()

			if err != nil {
				// failed, wait
				p.wg.Done()
				continue
			}

			p.free <- conn
			deadConn--

			// try to dead the rest of conns
			for i := 0; i < deadConn; i++ {
				p.dead <- nil
				deadConn--
			}
			p.wg.Done()

		case conn := <-p.dead:
			if closer, ok := conn.(io.Closer); ok {
				closer.Close()
			}

			if !p.acquireLock() {
				// pool is dead
				return
			}

			// immediate retry attempt
			conn, err := p.createConn()

			if err != nil {
				p.wg.Done()
				deadConn++
				continue
			}

			p.free <- conn
			p.wg.Done()
		}
	}
}

// close the connection and dead it with new one (if pool is still alive).
func (p *connPool) replaceConn(conn interface{}) {
	p.muc.Lock()
	for i, c := range p.conns {
		if conn == c {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			break
		}
	}
	p.muc.Unlock()

	p.dead <- conn
}

// Creates new connection in the pool.
func (p *connPool) createConn() (interface{}, error) {
	c, err := p.new()
	if err != nil {
		return nil, err
	}

	p.muc.Lock()
	p.conns = append(p.conns, c)
	p.muc.Unlock()

	return c, nil
}

// indicate that we have reserved the operation (pool must wait until operation is complete)
func (p *connPool) acquireLock() bool {
	p.muw.Lock()
	defer p.muw.Unlock()

	if atomic.LoadInt32(&p.inDestroy) != 0 {
		return false
	}

	p.wg.Add(1)
	return true
}
