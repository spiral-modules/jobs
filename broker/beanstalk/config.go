package beanstalk

import (
	"errors"
	"github.com/spiral/roadrunner/service"
	"strings"
	"syscall"
	"time"
)

// Config defines beanstalk broker configuration.
type Config struct {
	// Addr of beanstalk server.
	Addr string

	// Timeout to allocate the connection. Default 10 seconds.
	Timeout int
}

// Hydrate config values.
func (c *Config) Hydrate(cfg service.Config) error {
	return cfg.Unmarshal(c)
}

// TimeoutDuration returns number of seconds allowed to allocate the connection.
func (c *Config) TimeoutDuration() time.Duration {
	timeout := c.Timeout
	if timeout == 0 {
		timeout = 10
	}

	return time.Duration(timeout) * time.Second
}

// size creates new rpc socket Listener.
func (c *Config) newConn() (*conn, error) {
	dsn := strings.Split(c.Addr, "://")
	if len(dsn) != 2 {
		return nil, errors.New("invalid socket DSN (tcp://localhost:11300, unix://beanstalk.sock)")
	}

	if dsn[0] == "unix" {
		syscall.Unlink(dsn[1])
	}

	return newConn(dsn[0], dsn[1], c.TimeoutDuration())
}
