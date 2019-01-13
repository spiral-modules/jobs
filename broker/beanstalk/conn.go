package beanstalk

import (
	"fmt"
	"github.com/beanstalkd/go-beanstalk"
	"strings"
	"sync"
	"time"
)

var connErrors = []string{"pipe", "read tcp", "write tcp", "EOF"}

// creates new connections
type connFactory interface {
	newConn() (*conn, error)
}

// conn protects allocation for one connection between
// threads and provides reconnecting capabilities.
type conn struct {
	tout time.Duration
	conn *beanstalk.Conn
	free chan interface{}
	dead chan interface{}
	stop chan interface{}
	lock *sync.Cond
}

// creates new beanstalk connection and reconnect watcher.
func newConn(network, addr string, tout time.Duration) (cn *conn, err error) {
	cn = &conn{
		tout: tout,
		free: make(chan interface{}, 1),
		dead: make(chan interface{}, 1),
		stop: make(chan interface{}),
		lock: sync.NewCond(&sync.Mutex{}),
	}

	cn.conn, err = beanstalk.Dial(network, addr)
	if err != nil {
		return nil, err
	}

	go cn.watch(network, addr)

	return cn, nil
}

// close the connection and reconnect watcher.
func (cn *conn) Close() error {
	cn.lock.L.Lock()
	defer cn.lock.L.Unlock()

	close(cn.stop)
	for cn.free != nil {
		cn.lock.Wait()
	}

	return nil
}

// acquire connection instance or return error in case of timeout.
func (cn *conn) acquire() (*beanstalk.Conn, error) {
	select {
	case <-cn.stop:
		return nil, fmt.Errorf("connection closed")
	case <-cn.free:
		return cn.conn, nil
	default:
		// try with timeout
		tout := time.NewTimer(cn.tout)
		select {
		case <-cn.stop:
			tout.Stop()
			return nil, fmt.Errorf("connection closed")
		case <-cn.free:
			tout.Stop()
			return cn.conn, nil
		case <-tout.C:
			return nil, fmt.Errorf("unable to allocate connection (timeout %s)", cn.tout)
		}
	}
}

// release acquired connection.
func (cn *conn) release(err error) error {
	if isConnError(err) {
		// reconnect is required
		cn.dead <- err
	} else {
		cn.free <- nil
	}

	return err
}

// watch and reconnect if dead
func (cn *conn) watch(network, addr string) {
	cn.free <- nil
	for {
		select {
		case <-cn.dead:
			// try to reconnect
			if conn, err := beanstalk.Dial(network, addr); err == nil {
				cn.conn = conn
				cn.free <- nil
				continue
			}

			// retry later
			cn.dead <- nil
			time.Sleep(cn.tout)

		case <-cn.stop:
			cn.lock.L.Lock()
			select {
			case <-cn.dead:
			case <-cn.free:
			}

			// stop underlying connection
			cn.conn.Close()

			cn.free = nil
			cn.lock.Signal()
			cn.lock.L.Unlock()

			return
		}
	}
}

// isConnError indicates that error is related to dead socket.
func isConnError(err error) bool {
	if err == nil {
		return false
	}

	for _, errStr := range connErrors {
		// golang...
		if strings.Contains(err.Error(), errStr) {
			return true
		}
	}

	return false
}
