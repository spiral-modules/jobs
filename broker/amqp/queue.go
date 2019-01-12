package amqp

import (
	"errors"
	"fmt"
	"github.com/spiral/jobs"
	"github.com/streadway/amqp"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type queue struct {
	active int32
	pipe   *jobs.Pipeline

	exchange, name string
	consumer       string
	publishPool    *chanPool
	consumePool    *chanPool

	// tube events
	lsn func(event int, ctx interface{})

	// active operations
	muw sync.RWMutex
	wg  sync.WaitGroup

	// exec handlers
	running  int32
	execPool chan jobs.Handler
	err      jobs.ErrorHandler
}

// newQueue creates new queue wrapper for AMQP.
func newQueue(pipe *jobs.Pipeline, publish, consume *chanPool, lsn func(event int, ctx interface{})) (*queue, error) {
	if pipe.String("exchange", "amqp.direct") == "" {
		return nil, errors.New("missing `exchange` parameter on amqp pipeline")
	}

	if pipe.String("queue", "") == "" {
		return nil, errors.New("missing `queue` parameter on amqp pipeline")
	}

	if pipe.Integer("prefetch", 1) == 0 {
		return nil, errors.New("queue `prefetch` option can not be 0")
	}

	return &queue{
		exchange:    pipe.String("exchange", "amqp.direct"),
		name:        pipe.String("queue", ""),
		consumer:    pipe.String("consumer", fmt.Sprintf("rr-jobs:%s-%v", pipe.Name(), os.Getpid())),
		pipe:        pipe,
		publishPool: publish,
		consumePool: consume,
		lsn:         lsn,
	}, nil
}

// associate queue with new consumePool pool
func (q *queue) configure(execPool chan jobs.Handler, err jobs.ErrorHandler) error {
	q.execPool = execPool
	q.err = err

	return q.declare(q.name, q.name, nil)
}

func (q *queue) serve() {
	atomic.StoreInt32(&q.active, 1)

	var (
		breakConsuming = false
		breakDelivery  = false
	)

	for {
		<-q.consumePool.ensureConnection()
		if atomic.LoadInt32(&q.active) == 0 {
			// stopped
			return
		}

		// isolate (!)
		c, err := q.consumePool.channel(q.name)
		if err != nil {
			q.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: q.pipe, Caused: err})
			return
		}

		if err := c.ch.Qos(q.pipe.Integer("prefetch", 1), 0, false); err != nil {
			q.consumePool.release(c, err)
			q.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: q.pipe, Caused: err})
			return
		}

		delivery, err := c.ch.Consume(q.name, q.consumer, false, false, false, false, nil)
		if err != nil {
			q.consumePool.release(c, err)
			q.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: q.pipe, Caused: err})
			return
		}

		breakDelivery = false
		breakConsuming = false
		for {
			select {
			case err := <-c.signal:
				// channel error, we need new channel
				q.consumePool.release(c, err)
				breakConsuming = true

				if err == nil {
					// graceful stop signal
					breakDelivery = true
				} else {
					q.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: q.pipe, Caused: err})
				}

			case d := <-delivery:
				if d.Body == nil {
					// consuming has been closed?
					breakConsuming = true
					break
				}

				q.wg.Add(1)
				h := <-q.execPool

				go func(h jobs.Handler, d amqp.Delivery) {
					err := q.do(h, d)
					q.wg.Done()

					if err != nil {
						q.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: q.pipe, Caused: err})
					}
				}(h, d)
			}

			if breakConsuming {
				break
			}
		}

		if breakDelivery {
			return
		}
	}
}

func (q *queue) do(h jobs.Handler, d amqp.Delivery) error {
	atomic.AddInt32(&q.running, 1)
	defer atomic.AddInt32(&q.running, ^int32(0))

	id, attempt, j, err := unpack(d)
	if err != nil {
		q.execPool <- h
		q.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: q.pipe, Caused: err})
		return d.Nack(false, false)
	}

	err = h(id, j)
	q.execPool <- h

	if err == nil {
		// success
		return d.Ack(false)
	}

	// failed
	q.err(id, j, err)

	if !j.Options.CanRetry(attempt) {
		return d.Nack(false, false)
	}

	// retry as new j (to accommodate attempt number and new delay)
	if err = q.publish(id, attempt+1, j, j.Options.RetryDuration()); err != nil {
		q.throw(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: q.pipe, Caused: err})
		return d.Nack(false, true)
	}

	return d.Ack(false)
}

func (q *queue) stop() {
	if atomic.LoadInt32(&q.active) == 0 {
		return
	}

	atomic.StoreInt32(&q.active, 0)

	c, err := q.consumePool.channel(q.name)
	if err == nil {
		err = c.ch.Cancel(q.consumer, true)
	}

	q.muw.Lock()
	q.wg.Wait() // wait for all the jobs to complete
	q.muw.Unlock()

	// we can release channel now
	q.consumePool.release(c, err)
}

// publish message to queue or to delayed queue.
func (q *queue) publish(id string, attempt int, j *jobs.Job, delay time.Duration) error {
	c, err := q.publishPool.channel(q.name)
	if err != nil {
		return err
	}

	queueName := q.name

	if delay != 0 {
		delayMs := int64(delay.Seconds() * 1000)
		queueName = fmt.Sprintf("delayed-%d.%s.%s", delayMs, q.exchange, q.name)

		err := q.declare(
			queueName,
			queueName,
			amqp.Table{
				"x-dead-letter-exchange":    q.exchange,
				"x-dead-letter-routing-key": q.name,
				"x-message-ttl":             delayMs,
				"x-expires":                 delayMs * 2,
			},
		)

		if err != nil {
			return err
		}
	}

	err = c.ch.Publish(
		q.exchange, // exchange
		queueName,  // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			Body:         j.Body(),
			DeliveryMode: amqp.Persistent,
			Headers:      pack(id, attempt, j),
		},
	)

	if err != nil {
		go q.publishPool.release(c, err)
	}

	return err
}

// declare queue and binding to it
func (q *queue) declare(queue string, key string, args amqp.Table) error {
	c, err := q.publishPool.channel(q.name)
	if err != nil {
		return err
	}

	err = c.ch.ExchangeDeclare(q.exchange, "direct", true, false, false, false, nil)
	if err != nil {
		go q.publishPool.release(c, err)
		return err
	}

	_, err = c.ch.QueueDeclare(queue, true, false, false, false, args)
	if err != nil {
		go q.publishPool.release(c, err)
		return err
	}

	err = c.ch.QueueBind(queue, key, q.exchange, false, nil)
	if err != nil {
		go q.publishPool.release(c, err)
	}

	return err
}

// inspect the queue
func (q *queue) inspect() (*amqp.Queue, error) {
	c, err := q.publishPool.channel(q.name)
	if err != nil {
		return nil, err
	}

	queue, err := c.ch.QueueInspect(q.name)
	if err != nil {
		go q.publishPool.release(c, err)
	}

	return &queue, err
}

// throw handles service, server and pool events.
func (q *queue) throw(event int, ctx interface{}) {
	q.lsn(event, ctx)
}
