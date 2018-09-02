package broker

import (
	"github.com/spiral/roadrunner/service"
	"strings"
	"syscall"
	"github.com/beanstalkd/go-beanstalk"
	"errors"
)

// RedisConfig defines connection options to Redis server.
type BeanstalkConfig struct {
	// Enable to disable service.
	Enable bool

	// Address of beanstalk server.
	Address string
}

// Hydrate populates config with values.
func (c *BeanstalkConfig) Hydrate(cfg service.Config) error {
	return cfg.Unmarshal(&c)
}

// Listener creates new rpc socket Listener.
func (c *BeanstalkConfig) Conn() (*beanstalk.Conn, error) {
	dsn := strings.Split(c.Address, "://")
	if len(dsn) != 2 {
		return nil, errors.New("invalid socket DSN (tcp://:6001, unix://rpc.sock)")
	}

	if dsn[0] == "unix" {
		syscall.Unlink(dsn[1])
	}

	return beanstalk.Dial(dsn[0], dsn[1])
}
