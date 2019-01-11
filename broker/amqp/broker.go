package amqp

import (
	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"github.com/spiral/jobs"
	"github.com/streadway/amqp"
	"sync"
)

type Broker struct {
	conn        *amqp.Connection
	cfg         *Config
	mu          sync.Mutex
	stop        chan interface{}
	queue       map[*jobs.Pipeline]*Queue
	wg          sync.WaitGroup
	handlerPool chan jobs.Handler
	err         jobs.ErrorHandler
}

// Init configures local job broker.
func (b *Broker) Init(cfg *Config) (bool, error) {
	b.cfg = cfg
	return true, nil
}

// Listen configures broker with list of tubes to listen and handler function. Local broker groups all tubes
// together.
func (b *Broker) Listen(pipelines []*jobs.Pipeline, pool chan jobs.Handler, err jobs.ErrorHandler) error {
	b.queue = make(map[*jobs.Pipeline]*Queue)
	for _, p := range pipelines {
		if err := b.registerQueue(p); err != nil {
			return err
		}
	}

	b.handlerPool = pool
	b.err = err
	return nil
}

// registerTube new beanstalk pipeline
func (b *Broker) registerQueue(pipeline *jobs.Pipeline) error {
	queue, err := NewQueue(pipeline)
	if err != nil {
		return err
	}

	b.queue[pipeline] = queue
	return nil
}

// Push new job to queue
func (b *Broker) Push(p *jobs.Pipeline, j *jobs.Job) (string, error) {
	channel, err := b.conn.Channel()
	if err != nil {
		return "", err
	}
	defer channel.Close()

	// TODO: Figure out how to send to Exchange, not Queue.

	if err := b.queue[p].SendMessage(channel, j); err != nil {
		return "", err
	}

	return "NotSupported", nil
}

func (b *Broker) Serve() (err error) {
	b.conn, err = b.cfg.AMQP()
	if err != nil {
		return err
	}

	if err := b.cfg.initExchanges(b.conn); err != nil {
		return err
	}

	b.mu.Lock()
	b.stop = make(chan interface{})
	b.mu.Unlock()

	for _, q := range b.queue {
		if q.Queue == "" {
			return errors.New("pipeline queue name cannot be blank")
		}

		if q.Create {
			if err := b.createQueue(q); err != nil {
				return err
			}
		}

		if q.Listen {
			b.wg.Add(1)
			go b.listen(q)
		}
	}

	b.wg.Wait()
	<-b.stop

	return nil
}

// Stop serving.
func (b *Broker) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.stop != nil {
		close(b.stop)
	}
}

func (b *Broker) createQueue(q *Queue) error {
	channel, err := b.conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	if _, err := channel.QueueDeclare(q.Queue, q.Durable, q.AutoDelete, q.Exclusive, q.NoWait, q.Arguments); err != nil {
		return err
	}
	for _, b := range q.Binds {
		if err := channel.QueueBind(q.Queue, b.Key, b.Exchange, b.NoWait, b.Arguments); err != nil {
			return err
		}
	}
	return nil
}

func (b *Broker) listen(q *Queue) {
	channel, err := b.conn.Channel()
	if err != nil {
		return
	}
	defer channel.Close()
	defer b.wg.Done()

	var job *jobs.Job
	var handler jobs.Handler

	consumer, err := channel.Consume(
		q.Queue,
		"",
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)

	for {
		select {
		case <-b.stop:
			return
		case msg := <-consumer:
			handler = <-b.handlerPool

			err = json.Unmarshal(msg.Body, &job)
			if err != nil {
				// need additional logging
				continue
			}

			go func() {
				jerr := handler("NotSupported", job)
				b.handlerPool <- handler

				if jerr == nil {
					return
				}

				if !job.CanRetry() {
					b.err("NotSupported", job, jerr)
					return
				}

				if err := q.SendMessage(channel, job); err != nil {
					b.err("NotSupported", job, fmt.Errorf("error re-queuing message: %s", err))
					return
				}
			}()
		}
	}
}
