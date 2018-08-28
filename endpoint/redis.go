package endpoint

import (
	"github.com/spiral/jobs"
	"github.com/spiral/roadrunner/service"
	"github.com/go-redis/redis"
	"time"
	"encoding/json"
	"sync"
	"fmt"
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
	cfg       *RedisConfig
	client    *redis.Client
	exec      jobs.Handler
	mu        sync.Mutex
	pipelines map[*jobs.Pipeline]*pipeline
	stop      chan interface{}
}

// redis specific pipeline configuration
type pipeline struct {
	// Listen the pipeline.
	Listen bool

	// Queue name.
	Queue string

	// Mode defines operating mode (fifo, lifo, broadcast)
	Mode string

	// Timeout defines listen timeout, defaults to 1.
	Timeout int
}

func makePipeline(p *jobs.Pipeline) (*pipeline, error) {
	rp := &pipeline{
		Listen:  p.Listen,
		Queue:   p.Options.String("queue", ""),
		Mode:    p.Options.String("mode", "fifo"),
		Timeout: p.Options.Int("timeout", 1),
	}

	if err := rp.Valid(); err != nil {
		return nil, err
	}

	return rp, nil
}

// Valid returns error if pipeline configuration is not valid.
func (p *pipeline) Valid() error {
	if p.Queue == "" {
		return fmt.Errorf("missing `queue` option for redis pipeline")
	}

	if p.Mode != "fifo" && p.Mode != "lifo" {
		return fmt.Errorf("undefined pipeline mode `%s` [fifo|lifo]", p.Mode)
	}

	if p.Timeout < 0 {
		return fmt.Errorf("invalid pipeline timeout %v", p.Timeout)
	}

	return nil
}

// Handle configures endpoint with list of pipelines to listen and handler function.
func (r *Redis) Handle(pipelines []*jobs.Pipeline, exec jobs.Handler) error {
	r.pipelines = make(map[*jobs.Pipeline]*pipeline)

	for _, p := range pipelines {
		if rp, err := makePipeline(p); err != nil {
			return err
		} else {
			r.pipelines[p] = rp
		}
	}

	r.exec = exec
	return nil
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

	// todo: delays

	var cmd *redis.IntCmd
	switch r.pipelines[p].Mode {
	case "fifo":
		cmd = r.client.RPush(r.pipelines[p].Queue, data)
	case "lifo":
		cmd = r.client.LPush(r.pipelines[p].Queue, data)
	}

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

	for _, p := range r.pipelines {
		if p.Listen {
			go r.listen(p)
		}
	}

	<-r.stop
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

func (r *Redis) listen(p *pipeline) error {
	for {
		select {
		case <-r.stop:
			return nil
		default:
			res := r.client.BLPop(time.Second*time.Duration(p.Timeout), p.Queue)
			if len(res.Val()) == 2 {
				j := &jobs.Job{}
				if err := json.Unmarshal([]byte(res.Val()[1]), j); err != nil {
					return err
				}

				go r.exec(j)
			}
		}
	}
}
