package jobs

import (
	"errors"
	"log"
	"sync"
)

// ConnErr indicates that given error is connection specific.
func ConnErr(err error) error {
	return connErr{err}
}

// ConnErr wraps regular error.
type connErr struct {
	// Underlying error
	err error
}

// Error return error code.
func (e connErr) Error() string {
	return e.err.Error()
}

// ConnPool manages set of connections with ability to
// retry operation using another connection in case of pipe error.
type ConnPool struct {
	// NumConn number of open connections.
	NumConn int

	// Open new connection
	Open func() (interface{}, error)

	// Close dead connection
	Close func(interface{})

	// active connections
	wg   sync.WaitGroup
	conn chan interface{}
}

// Init creates given number of pending connections.
func (p *ConnPool) Init() error {
	p.conn = make(chan interface{}, p.NumConn)

	for i := 0; i < p.NumConn; i++ {
		c, err := p.Open()
		if err != nil {
			go p.Destroy()
			return err
		}

		p.conn <- c
	}

	return nil
}

// Exec executes given function and provides ability to retry it in case of connection error.
func (p *ConnPool) Exec(exec func(interface{}) error) (err error) {
	p.wg.Add(1)
	defer p.wg.Done()

	var c interface{}
	for i := 0; i < p.NumConn; i++ {
		c = <-p.conn

		if c == nil {
			return errors.New("pool is empty")
		}

		err = exec(c)

		// non connection issue
		if _, ok := err.(connErr); !ok {
			p.conn <- c
			return err
		}

		go func(c interface{}) {
			log.Println("retry")

			// reconnecting
			p.Close(c)

			c, err := p.Open()
			if err != nil {
				// unable to reconnect
				p.Destroy()
			}

			// refilled
			p.conn <- c
		}(c)

		continue
	}

	return err
}

// Close all underlying connections.
func (p *ConnPool) Destroy() {
	p.wg.Wait()

	if p.conn == nil {
		return
	}

	for i := 0; i < p.NumConn; i++ {
		p.Close(<-p.conn)
	}

	close(p.conn)
	p.conn = nil
}
