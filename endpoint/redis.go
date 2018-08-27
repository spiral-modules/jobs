package endpoint

import (
	"github.com/spiral/jobs"
	"github.com/spiral/roadrunner/service"
	"github.com/go-redis/redis"
	"log"
	"time"
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
}

// SetHandler configures function to handle job execution.
func (r *Redis) Handler(exec jobs.Handler) {
	r.exec = exec
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
func (r *Redis) Push(j *jobs.Job) error {
	return nil
}

// Serve local endpoint.
func (r *Redis) Serve() error {
	// verify db connection
	_, err := r.client.Ping().Result()
	if err != nil {
		return err
	}

	//log.Println(r.client.RPush("pipe", "message"))
	log.Println(r.client.BLPop(time.Second, "pipe"))

	return nil
}

// Stop local endpoint.
func (r *Redis) Stop() {

}
