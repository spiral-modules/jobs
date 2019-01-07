package cpool

import (
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// ConnPool manages set of connections with ability to retry operation using another connection in case of pipe error.
type ConnPool struct {
	// Size number of open connections.
	Size int

	// Reconnect defines the time to wait between reconnect attempts.
	Reconnect time.Duration

	// New creates new connection or returns error.
	New func() (io.Closer, error)

	// Notify chan interface{}

	// active connections
	muc  sync.Mutex
	conn []io.Closer

	// disable refilling
	inDestroy int32

	// active allocations
	muw sync.Mutex
	wg  sync.WaitGroup

	// free connections
	free      chan interface{}
	reconnect chan interface{}

	// stop the pool
	wait chan interface{}
}

// Start creates given number of pending connections. Non thread safe.
func (p *ConnPool) Start() error {
	p.wait = make(chan interface{})
	p.conn = make([]io.Closer, 0, p.Size)

	p.free = make(chan interface{}, p.Size)
	p.reconnect = make(chan interface{}, p.Size)

	start := make(chan interface{}, p.Size)
	for i := 0; i < p.Size; i++ {
		go func() {
			c, err := p.createConn()
			if err != nil {
				start <- err
			}

			p.free <- c
			start <- nil
		}()
	}

	for i := 0; i < p.Size; i++ {
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
func (p *ConnPool) Allocate(tout time.Duration) (interface{}, error) {
	p.muw.Lock()
	defer p.muw.Unlock()

	select {
	case <-p.wait:
		return nil, fmt.Errorf("unable to allocate connection (pool closed)")
	case c := <-p.free:
		p.wg.Add(1)
		return c, nil
	default:
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

// Release the connection with or without context error. If error is instance of ConnError then
// connection will be recreated.
func (p *ConnPool) Release(conn interface{}, err error) {
	p.wg.Done()

	// connection issue, retry?
	if _, ok := err.(ConnError); ok {
		p.replaceConn(conn)
		return
	}

	p.free <- conn
}

// Destroy pool and close all underlying connections. Thread safe.
func (p *ConnPool) Destroy() {
	if atomic.LoadInt32(&p.inDestroy) == 1 {
		return
	}

	atomic.AddInt32(&p.inDestroy, 1)

	close(p.wait)
	p.muw.Lock()
	p.wg.Wait()
	p.muw.Unlock()
	close(p.free)

	p.muc.Lock()
	defer p.muc.Unlock()
	for _, c := range p.conn {
		c.Close()
	}

	p.conn = nil
}

// serve connections and reconnect dead sockets.
func (p *ConnPool) serve() {
	var (
		reconnect = time.NewTicker(p.Reconnect)
		deadConn  = 0
	)

	for {
		select {
		case <-p.wait:
			reconnect.Stop()
			return
		case <-reconnect.C:
			if deadConn == 0 {
				// nothing to reconnect
				continue
			}

			if !p.acquireLock() {
				// pool is dead, chimichanga
				return
			}

			conn, err := p.createConn()

			if err != nil {
				// unable to reconnect, still
				p.wg.Done()
				continue
			}

			p.free <- conn
			deadConn--

			// try to reconnect the rest of sockets
			for i := 0; i < deadConn; i++ {
				p.reconnect <- nil
				deadConn--
			}
			p.wg.Done()

		case conn := <-p.reconnect:
			if closer, ok := conn.(io.Closer); ok {
				closer.Close()
			}

			if !p.acquireLock() {
				// pool is dead
				return
			}

			// immediate reconnect attempt
			conn, err := p.createConn()

			if err != nil {
				p.wg.Done()

				// try later
				deadConn++
				continue
			}

			p.free <- conn
			p.wg.Done()
		}
	}
}

// close the connection and reconnect it with new one (if pool is still alive).
func (p *ConnPool) replaceConn(conn interface{}) {
	p.muc.Lock()
	for i, c := range p.conn {
		if conn == c {
			p.conn = append(p.conn[:i], p.conn[i+1:]...)
			break
		}
	}
	p.muc.Unlock()

	p.reconnect <- conn
}

// Creates new connection in the pool.
func (p *ConnPool) createConn() (interface{}, error) {
	c, err := p.New()
	if err != nil {
		return nil, err
	}

	p.muc.Lock()
	p.conn = append(p.conn, c)
	p.muc.Unlock()

	return c, nil
}

// indicate that we have reserved the operation (pool must wait until operation is complete)
func (p *ConnPool) acquireLock() bool {
	p.muw.Lock()
	defer p.muw.Unlock()

	if atomic.LoadInt32(&p.inDestroy) != 0 {
		return false
	}

	p.wg.Add(1)
	return true
}
