package endpoint

import (
	"github.com/spiral/jobs"
	"github.com/spiral/roadrunner/service"
	"github.com/go-redis/redis"
	"time"
	"encoding/json"
	"sync"
)

// RedisConfig defines connection options to Redis server.
type RedisConfig struct {
	Enable   bool
	Address  string
	Password string
	DB       int
}

// Hydrate populates config with values.
func (c *RedisConfig) Hydrate(cfg service.Config) error {
	return cfg.Unmarshal(&c)
}

// Local run jobs using local goroutines.
type Redis struct {
	cfg    *RedisConfig
	client *redis.Client
	exec   jobs.Handler
	mu     sync.Mutex
	stop   chan interface{}
}

// SetHandler configures function to handle job execution.
func (r *Redis) Handle(pipes []*jobs.Pipeline, exec jobs.Handler) jobs.Endpoint {
	r.exec = exec
	return r
}

// Init configures local job endpoint.
func (r *Redis) Init(cfg *RedisConfig) (bool, error) {
	if !cfg.Enable {
		return false, nil
	}
	r.cfg = cfg

	r.client = redis.NewClient(&redis.Options{
		Addr:     r.cfg.Address,
		Password: r.cfg.Password,
		DB:       r.cfg.DB,
	})

	return true, nil
}

// Push new job to queue
func (r *Redis) Push(p *jobs.Pipeline, j *jobs.Job) error {
	data, err := json.Marshal(j)
	if err != nil {
		return err
	}

	cmd := r.client.RPush(j.Pipeline, data)
	if cmd.Err() != nil {
		return cmd.Err()
	}

	return nil
}

// Serve local endpoint.
func (r *Redis) Serve() error {
	// verify db connection
	_, err := r.client.Ping().Result()
	if err != nil {
		return err
	}

	r.mu.Lock()
	r.stop = make(chan interface{})
	r.mu.Unlock()

	for {
		select {
		case <-r.stop:
			return nil
		default:
			res := r.client.BLPop(time.Second, "redis")
			if len(res.Val()) == 2 {
				j := &jobs.Job{}
				if err := json.Unmarshal([]byte(res.Val()[1]), j); err != nil {
					return err
				}

				// log err
				r.exec(j)
			}
		}
	}

	return nil
}

// Stop local endpoint.
func (r *Redis) Stop() {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.stop != nil {
		close(r.stop)
	}
}
