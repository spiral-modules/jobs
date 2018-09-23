package beanstalk

import (
	"errors"
	"github.com/spiral/roadrunner/service"
	"github.com/xuri/aurora/beanstalk"
	"strings"
	"syscall"
)

// Config defines beanstalk broker configuration.
type Config struct {
	// Address of beanstalk server.
	Address string
}

// Hydrate config values.
func (c *Config) Hydrate(cfg service.Config) error {
	return cfg.Unmarshal(c)
}

// Listener creates new rpc socket Listener.
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
