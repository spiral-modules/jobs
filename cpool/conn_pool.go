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

	// New creates new connection or returns error.
	New func() (io.Closer, error)

	// Notify chan interface{}

	// active connections
	muc  sync.Mutex
	conn []io.Closer

	// disable refilling
	inDestroy int32

	// waits for creation, destruction and execs
	wg   sync.WaitGroup
	free chan interface{}
	wait chan interface{}
}

// Start creates given number of pending connections. Non thread safe.
func (p *ConnPool) Start() error {
	p.conn = make([]io.Closer, 0, p.Size)
	p.free = make(chan interface{}, p.Size)
	p.wait = make(chan interface{})

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

	return nil
}

// Close all underlying connections. Non thread safe.
func (p *ConnPool) Destroy() {
	atomic.AddInt32(&p.inDestroy, 1)

	close(p.wait)
	p.wg.Wait()
	close(p.free)

	p.muc.Lock()
	defer p.muc.Unlock()
	for _, c := range p.conn {
		c.Close()
	}

	p.conn = nil
}

// Exec executes given function and provides ability to retry it in case of connection error.
func (p *ConnPool) Exec(exec func(interface{}) error, tout time.Duration) (err error) {
	p.wg.Add(1)
	defer p.wg.Done()

	for i := 0; i < p.Size; i++ {
		c, err := p.allocateConn(tout)
		if err != nil {
			break
		}

		err = exec(c)

		// connection issue, retry?
		if _, ok := err.(ConnError); ok {
			go p.replaceConn(c)
			continue
		}

		p.free <- c
		break
	}

	return err
}

// allocateConn free connection or return error. Blocked until connection is available or pool is dead.
func (p *ConnPool) allocateConn(tout time.Duration) (io.Closer, error) {
	select {
	case <-p.wait:
		return nil, fmt.Errorf("unable to allocate connection (pool closed)")
	case c := <-p.free:
		return c.(io.Closer), nil
	default:
		timeout := time.NewTimer(tout)
		select {
		case <-timeout.C:
			return nil, fmt.Errorf("allocate connection timeout (%s)", tout)
		case <-p.wait:
			timeout.Stop()
			return nil, fmt.Errorf("unable to allocate connection (pool closed)")
		case c := <-p.free:
			timeout.Stop()
			return c.(io.Closer), nil
		}
	}
}

// Creates new connection in the pool.
func (p *ConnPool) createConn() (io.Closer, error) {
	p.wg.Add(1)
	defer p.wg.Done()

	c, err := p.New()
	if err != nil {
		return nil, err
	}

	p.muc.Lock()
	defer p.muc.Unlock()

	p.conn = append(p.conn, c)

	return c, nil
}

// close the connection and replace it with new one (if pool is still alive).
func (p *ConnPool) replaceConn(conn io.Closer) {
	p.wg.Add(1)
	defer p.wg.Done()

	conn.Close()

	p.muc.Lock()
	for i, c := range p.conn {
		if conn == c {
			p.conn = append(p.conn[:i], p.conn[i+1:]...)
			break
		}
	}
	p.muc.Unlock()

	if !p.destroying() {
		c, err := p.New()
		if err != nil {
			// connection is dead and we can not replace it, conn pool is not in stalled mode

			// todo: handle stalled (???)
			p.Destroy()

			return
		}

		p.free <- c
	}
}

// destroying indicates that pool is being destroyed
func (p *ConnPool) destroying() bool {
	return atomic.LoadInt32(&p.inDestroy) != 0
}
