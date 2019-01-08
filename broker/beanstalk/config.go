package beanstalk

import (
	"errors"
	"github.com/beanstalkd/go-beanstalk"
	"github.com/spiral/roadrunner/service"
	"io"
	"net"
	"strings"
	"syscall"
	"time"
)

// Config defines beanstalk broker configuration.
type Config struct {
	// Addr of beanstalk server.
	Addr string

	// size defines number of open connections to beanstalk server. Default 5.
	NumConn int

	// Prefetch number of jobs allowed to be fetched by each pipe at the same time. Default 1.
	Prefetch int

	// Reserve timeout in seconds. Default 1.
	Reserve int

	// Timeout to allocate the connection. Default 5.
	Timeout int
}

// InitDefaults sets missing values to their default values.
func (c *Config) InitDefaults() error {
	c.NumConn = 2
	c.Prefetch = 1
	c.Reserve = 1
	c.Timeout = 10

	return nil
}

// Hydrate config values.
func (c *Config) Hydrate(cfg service.Config) error {
	return cfg.Unmarshal(c)
}

// connPool creates new connection pool for beanstalk.
func (c *Config) ConnPool() *connPool {
	return &connPool{
		size:      c.NumConn,
		reconnect: c.TimeoutDuration(),
		new:       func() (i io.Closer, e error) { return c.newConn() },
	}
}

// size creates new rpc socket Listener.
func (c *Config) newConn() (*beanstalk.Conn, error) {
	dsn := strings.Split(c.Addr, "://")
	if len(dsn) != 2 {
		return nil, errors.New("invalid socket DSN (tcp://:6001, unix://rpc.sock)")
	}

	if dsn[0] == "unix" {
		syscall.Unlink(dsn[1])
	}

	conn, err := net.Dial(dsn[0], dsn[1])
	if err != nil {
		return nil, err
	}

	return beanstalk.NewConn(conn), nil
}

// ReserveDuration returns number of seconds to reserve the job.
func (c *Config) ReserveDuration() time.Duration {
	return time.Duration(c.Reserve) * time.Second
}

// TimeoutDuration returns number of seconds allowed to allocate the connection.
func (c *Config) TimeoutDuration() time.Duration {
	return time.Duration(c.Timeout) * time.Second
}
