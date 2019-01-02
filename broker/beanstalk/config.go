package beanstalk

import (
	"errors"
	"github.com/beanstalkd/go-beanstalk"
	"github.com/spiral/roadrunner/service"
	"strings"
	"syscall"
	"time"
)

// Config defines beanstalk broker configuration.
type Config struct {
	// Address of beanstalk server.
	Address string

	// Reserve timeout in seconds.
	Reserve int

	// Connections defines number of open connections to beanstalk server. Default 5.
	Connections int
}

// InitDefaults sets missing values to their default values.
func (c *Config) InitDefaults() error {
	c.Connections = 5
	return nil
}

// Hydrate config values.
func (c *Config) Hydrate(cfg service.Config) error {
	return cfg.Unmarshal(c)
}

// Conn creates new rpc socket Listener.
func (c *Config) Conn() (*beanstalk.Conn, error) {
	dsn := strings.Split(c.Address, "://")
	if len(dsn) != 2 {
		return nil, errors.New("invalid socket DSN (tcp://:6001, unix://rpc.sock)")
	}

	if dsn[0] == "unix" {
		syscall.Unlink(dsn[1])
	}

	return beanstalk.Dial(dsn[0], dsn[1])
}

// ReserveDuration returns number of seconds to reserve the job.
func (c *Config) ReserveDuration() time.Duration {
	return time.Duration(c.Reserve) * time.Second
}
