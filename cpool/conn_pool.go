package cpool

import (
	"errors"
	"io"
	"strings"
	"sync"
)

var connErrors = []string{"pipe", "read tcp", "write tcp", "EOF"}

// ConnError wraps regular error as pipe error and forces pool to re-init the connection.
type ConnError struct {
	// Error is underlying error.
	Err error
}

// Error return error code.
func (e ConnError) Error() string {
	return e.Err.Error()
}

// ConnPool manages set of connections with ability to retry operation using another connection in case of pipe error.
type ConnPool struct {
	// Size number of open connections.
	Size int

	// New creates new connection or returns error.
	New func() (io.Closer, error)

	// active connections
	wg   sync.WaitGroup
	conn chan interface{}
}

// Start creates given number of pending connections.
func (p *ConnPool) Start() error {
	p.conn = make(chan interface{}, p.Size)

	for i := 0; i < p.Size; i++ {
		c, err := p.New()
		if err != nil {
			p.Destroy()
			return err
		}

		p.conn <- c
	}

	// todo: do something with reconnect here

	return nil
}

// Exec executes given function and provides ability to retry it in case of connection error.
func (p *ConnPool) Exec(exec func(interface{}) error) (err error) {
	p.wg.Add(1)

	var c interface{}
	for i := 0; i < p.Size; i++ {
		// todo: also handle dead situation (!)
		c = <-p.conn

		if c == nil {
			return errors.New("pool is empty")
		}

		err = exec(c)

		// non connection issue
		if _, ok := err.(ConnError); !ok {
			p.wg.Done()
			p.conn <- c
			return err
		}

		// todo: move
		p.wg.Done()

		go func(c io.Closer) {
			c.Close()

			c, err := p.New()
			if err != nil {

				// unable to reconnect
				p.Destroy()

				// refilled
				p.conn <- c
			}
		}(c.(io.Closer))
	}

	return err
}

// Close all underlying connections.
func (p *ConnPool) Destroy() {
	p.wg.Wait()

	if p.conn == nil {
		return
	}

	for i := 0; i < p.Size; i++ {
		c := <-p.conn
		c.(io.Closer).Close()
	}

	close(p.conn)
}

// IsConnError indicates that error is related to dead socket.
func IsConnError(err error) bool {
	for _, errStr := range connErrors {
		// golang...
		if strings.Contains(err.Error(), errStr) {
			return true
		}
	}

	return false
}
