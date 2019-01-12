package amqp

import (
	"fmt"
	"github.com/spiral/jobs"
	"github.com/streadway/amqp"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type queue struct {
	active         int32
	pipe           *jobs.Pipeline
	exchange, name string
	consumer       string
	publishPool    *chanPool

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
func newQueue(pipe *jobs.Pipeline, lsn func(event int, ctx interface{})) (*queue, error) {
	if pipe.String("exchange", "amqp.direct") == "" {
		return nil, fmt.Errorf("missing `exchange` parameter on amqp pipeline")
	}

	if pipe.String("queue", "") == "" {
		return nil, fmt.Errorf("missing `queue` parameter on amqp pipeline")
	}

	if pipe.Integer("prefetch", 4) == 0 {
		return nil, fmt.Errorf("queue `prefetch` option can not be 0")
	}

	return &queue{
		exchange: pipe.String("exchange", "amqp.direct"),
		name:     pipe.String("queue", ""),
		consumer: pipe.String("consumer", fmt.Sprintf("rr-jobs:%s-%v", pipe.Name(), os.Getpid())),
		pipe:     pipe,
		lsn:      lsn,
	}, nil
}

// associate queue with new consumePool pool
func (q *queue) configure(execPool chan jobs.Handler, err jobs.ErrorHandler) error {
	q.execPool = execPool
	q.err = err

	return nil
}

// serve consumes queue
func (q *queue) serve(publishPool, consumePool *chanPool) {
	atomic.StoreInt32(&q.active, 1)

	q.publishPool = publishPool

	var (
		cc  *channel
		err error
	)

	// declare queue
	if err := q.declare(q.name, q.name, nil); err != nil {
		q.report(err)
		return
	}

serving:
	for {
		<-consumePool.waitConnected()
		if atomic.LoadInt32(&q.active) == 0 {
			// stopped
			break serving
		}

		// allocate channel for the consuming
		if cc, err = consumePool.channel(q.name); err != nil {
			q.report(err)
			break serving
		}

		if err := cc.ch.Qos(q.pipe.Integer("prefetch", 4), 0, false); err != nil {
			consumePool.release(cc, err)
			q.report(err)
			break serving
		}

		delivery, err := cc.ch.Consume(q.name, q.consumer, false, false, false, false, nil)
		if err != nil {
			consumePool.release(cc, err)
			q.report(err)
			break serving
		}

	consuming:
		for {
			select {
			case err := <-cc.signal:
				// channel error, we need new channel
				consumePool.release(cc, err)

				if err != nil {
					q.report(err)
				} else {
					// closed
					break serving
				}

				break consuming

			case d := <-delivery:
				if d.Body == nil {
					break consuming
				}

				q.wg.Add(1)
				atomic.AddInt32(&q.running, 1)
				h := <-q.execPool

				go func(h jobs.Handler, d amqp.Delivery) {
					err := q.do(h, d)

					atomic.AddInt32(&q.running, ^int32(0))
					q.execPool <- h
					q.wg.Done()

					if err != nil {
						q.report(err)
					}
				}(h, d)
			}
		}
	}

	if cc != nil {
		consumePool.release(cc, err)
	}
}

func (q *queue) do(h jobs.Handler, d amqp.Delivery) error {
	id, attempt, j, err := unpack(d)
	if err != nil {
		q.report(err)
		return d.Nack(false, false)
	}

	err = h(id, j)

	if err == nil {
		return d.Ack(false)
	}

	// failed
	q.err(id, j, err)

	if !j.Options.CanRetry(attempt) {
		return d.Nack(false, false)
	}

	// retry as new j (to accommodate attempt number and new delay)
	if err = q.publish(id, attempt+1, j, j.Options.RetryDuration()); err != nil {
		q.report(err)
		return d.Nack(false, true)
	}

	return d.Ack(false)
}

func (q *queue) stop() {
	if atomic.LoadInt32(&q.active) == 0 {
		return
	}

	atomic.StoreInt32(&q.active, 0)

	q.muw.Lock()
	q.wg.Wait() // wait for all the jobs to complete
	q.muw.Unlock()
}

// publishPool message to queue or to delayed queue.
func (q *queue) publish(id string, attempt int, j *jobs.Job, delay time.Duration) error {
	c, err := q.publishPool.channel(q.name)
	if err != nil {
		return err
	}

	qName := q.name

	if delay != 0 {
		delayMs := int64(delay.Seconds() * 1000)
		qName = fmt.Sprintf("delayed-%d.%s.%s", delayMs, q.exchange, q.name)

		err := q.declare(qName, qName, amqp.Table{
			"x-dead-letter-exchange":    q.exchange,
			"x-dead-letter-routing-key": q.name,
			"x-message-ttl":             delayMs,
			"x-expires":                 delayMs * 2,
		})

		if err != nil {
			return err
		}
	}

	err = c.ch.Publish(
		q.exchange, // exchange
		qName,      // routing key
		false,      // mandatory
		false,      // immediate
		amqp.Publishing{
			ContentType:  "application/octet-stream",
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
		q.publishPool.release(c, err)
	}

	return &queue, err
}

// throw handles service, server and pool events.
func (q *queue) report(err error) {
	q.lsn(jobs.EventPipelineError, &jobs.PipelineError{Pipeline: q.pipe, Caused: err})
}
