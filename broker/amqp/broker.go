package amqp

import (
	"github.com/spiral/jobs"
	"sync"
	"github.com/streadway/amqp"
	"fmt"
	"errors"
	"strconv"
	"encoding/json"
	"time"
)

// Broker run queue using local goroutines.
type Broker struct {
	cfg      *Config
	mu       sync.Mutex
	stop     chan interface{}
	conn     *amqp.Connection
	channel  *amqp.Channel
	wg       sync.WaitGroup
	exe      jobs.Handler
	err      jobs.ErrorHandler
	confirms chan amqp.Confirmation
	queue    amqp.Queue
}

// Listen configures broker with list of pipelines to listen and handler function. Broker broker groups all pipelines
// together.
func (b *Broker) Listen(pipelines []*jobs.Pipeline, exe jobs.Handler, err jobs.ErrorHandler) error {
	b.exe = exe
	b.err = err

	return nil
}

// Init configures local job broker.
func (b *Broker) Init(cfg *Config) (bool, error) {
	b.cfg = cfg

	return true, nil
}

// Serve local broker.
func (b *Broker) Serve() error {
	conn, err := b.cfg.Conn()
	if err != nil {
		return err
	}
	defer conn.Close()

	b.conn = conn
	b.channel, err = conn.Channel()
	if err != nil {
		return err
	}
	defer b.channel.Close()

	if err := b.channel.ExchangeDeclare(
		"default", // name
		"direct",  // type
		true,      // durable
		false,     // auto-deleted
		false,     // internal
		false,     // noWait
		nil,       // arguments
	); err != nil {
		return fmt.Errorf("Exchange Declare: %s", err)
	}

	if err := b.channel.Confirm(false); err != nil {
		return fmt.Errorf("Channel could not be put into confirm mode: %s", err)
	}

	args := make(amqp.Table)
	args["x-delayed-type"] = "direct"
	b.queue, err = b.channel.QueueDeclare(
		"default", // name of the queue
		true,      // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // noWait
		args,      // arguments
	)

	if err != nil {
		return fmt.Errorf("Queue Declare: %s", err)
	}

	if err = b.channel.QueueBind(
		b.queue.Name, // name of the queue
		"default",    // bindingKey
		"default",    // sourceExchange
		false,        // noWait
		nil,          // arguments
	); err != nil {
		return fmt.Errorf("Queue Bind: %s", err)
	}

	deliveries, err := b.channel.Consume(
		"default", // name
		"rr",      // consumerTag,
		false,     // noAck
		false,     // exclusive
		false,     // noLocal
		false,     // noWait
		nil,       // arguments
	)
	if err != nil {
		return fmt.Errorf("Queue Consume: %s", err)
	}

	b.wg.Add(1)
	go b.handle(deliveries)

	b.mu.Lock()
	b.stop = make(chan interface{})
	b.mu.Unlock()

	b.confirms = b.channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	<-b.stop
	b.wg.Wait()

	return nil
}

// Stop local broker.
func (b *Broker) Stop() {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.stop != nil {
		close(b.stop)
	}
}

// Push new job to queue
func (b *Broker) Push(p *jobs.Pipeline, j *jobs.Job) (string, error) {

	jb, _ := json.Marshal(j)

	if err := b.channel.Publish(
		"default", // publish to an exchange
		"default", // routing to 0 or more queues
		false,     // mandatory
		false,     // immediate

		amqp.Publishing{
			Timestamp:       time.Now(),
			Headers:         amqp.Table{"x-delay": int64(j.Options.Delay * 1000)},
			ContentType:     "application/json",
			ContentEncoding: "",
			Body:            jb,
			DeliveryMode:    amqp.Persistent, // 1=non-persistent, 2=persistent
			Priority:        0,               // 0-9
		},
	); err != nil {
		return "", fmt.Errorf("Exchange Publish: %s", err)
	}

	return confirmOne(b.confirms)
}

func confirmOne(confirms <-chan amqp.Confirmation) (string, error) {
	if confirmed := <-confirms; confirmed.Ack {
		return strconv.FormatUint(confirmed.DeliveryTag, 10), nil
	} else {
		return "", errors.New("failed job delivery")
	}
}

func (b *Broker) handle(deliveries <-chan amqp.Delivery) {
	defer b.wg.Done()
	var job *jobs.Job

	for {
		select {
		case <-b.stop:
			return
		case d := <-deliveries:
			err := json.Unmarshal(d.Body, &job)
			if err != nil {
				// need additional logging
				continue
			}

			// local broker does not support job timeouts yet
			err = b.exe(fmt.Sprintf("%v", d.DeliveryTag), job)
			if err == nil {
				d.Ack(false)
				continue
			}

			if !job.CanRetry() {
				//	b.conn.Bury(id, 0)
				continue
			}

			// retry
			//b.conn.Release(id, 0, job.Options.RetryDuration())

			//	log.Printf(
			//		"got %dB delivery: [%v] %q",
			//		len(d.Body),
			//		d.DeliveryTag,
			//		d.Body,
			//	)
			//d.Ack(false)
		}
	}
}
