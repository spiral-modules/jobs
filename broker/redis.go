package broker

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
// @see https://hackage.haskell.org/package/hworker
// @see https://github.com/illuminate/queue
type Redis struct {
	cfg       *RedisConfig
	client    redis.Cmdable
	exec      jobs.Handler
	fail      jobs.ErrorHandler
	mu        sync.Mutex
	pipelines map[*jobs.Pipeline]*pipeline
	stop      chan interface{}
}

// Handle configures broker with list of pipelines to listen and handler function.
func (r *Redis) Handle(pipelines []*jobs.Pipeline, h jobs.Handler, f jobs.ErrorHandler) error {
	r.pipelines = make(map[*jobs.Pipeline]*pipeline)

	for _, p := range pipelines {
		rp, err := makePipeline(p)
		if err != nil {
			return err
		}

		r.pipelines[p] = rp
	}

	r.exec = h
	r.fail = f
	return nil
}

// Init configures local job broker.
func (r *Redis) Init(cfg *RedisConfig) (bool, error) {
	if !cfg.Enable {
		return false, nil
	}
	r.cfg = cfg

	// todo: cluster support
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

// Serve local broker.
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

// Stop local broker.
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

				r.exec(j)
			}
		}
	}
}
