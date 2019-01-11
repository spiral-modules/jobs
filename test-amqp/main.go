package main

import (
	"fmt"
	"github.com/streadway/amqp"
	"log"
	"sync"
	"time"
)

type Conn struct {
	addr, exchange string
	tout           time.Duration
	mu             sync.Mutex
	conn           *amqp.Connection
	muc            sync.Mutex
	channels       map[string]*channel
	retry          chan interface{}
}

type channel struct {
	ch       *amqp.Channel
	consumer string
	close    chan error
}

func NewConn(addr string, exchange string, tout time.Duration) (*Conn, error) {
	conn, err := amqp.Dial(addr)
	if err != nil {
		return nil, err
	}

	cn := &Conn{
		addr:     addr,
		exchange: exchange,
		tout:     tout,
		conn:     conn,
		channels: make(map[string]*channel),
	}

	go func() {
		for {
			select {
			case err := <-cn.conn.NotifyClose(make(chan *amqp.Error)):
				cn.retry = make(chan interface{})
				for _, ch := range cn.channels {
					ch.close <- err
				}

				// wipe all channels (!)
				cn.channels = make(map[string]*channel)
				log.Println("retrying")

				for {
					time.Sleep(tout)
					conn, err := amqp.Dial(addr)

					if err != nil {
						log.Println("unable to reconnect ", err)
						continue
					}

					// conn has been established!
					cn.conn = conn
					log.Println("reconnected! ")

					break
				}

				close(cn.retry)

				return
			}
		}
	}()

	return cn, err
}

// channel allocates new named connection channel. Each channel receiver must
// manage channel state on it's own!
func (cn *Conn) Channel(name string) (*channel, error) {
	cn.muc.Lock()
	defer cn.muc.Unlock()

	if ch, ok := cn.channels[name]; ok {
		return ch, nil
	}

	// we must create new channel
	ch, err := cn.conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		cn.exchange, // name
		"direct",    // type
		true,        // durable
		false,       // auto-deleted
		false,       // internal
		false,       // noWait
		nil,         // arguments
	)

	if err != nil {
		ch.Close()
		return nil, err
	}

	cn.channels[name] = &channel{
		ch:       ch,
		consumer: "",
		close:    make(chan error),
	}

	return cn.channels[name], nil
}

func (cn *Conn) Producer(queue string) (*amqp.Channel, error) {
	ch, err := cn.Channel(queue)
	if err != nil {
		return nil, err
	}

	_, err = ch.ch.QueueDeclare(
		queue, // name
		true,  // type
		false, // durable
		false, // auto-deleted
		false, // internal
		nil,
	)

	if err != nil {
		return nil, err
	}

	return ch.ch, nil
}

func (cn *Conn) Consumer(queue string) (<-chan amqp.Delivery, chan error, error) {
	ch, err := cn.Channel(queue)
	if err != nil {
		return nil, nil, err
	}

	_, err = ch.ch.QueueDeclare(
		queue, // name
		true,  // type
		false, // durable
		false, // auto-deleted
		false, // internal
		nil,
	)

	ch.consumer = "my-consumer"

	ch.ch.QueueBind(queue, queue, cn.exchange, false, nil)
	ch.ch.Qos(100, 0, false)

	delivery, err := ch.ch.Consume(
		"default",   // queue
		ch.consumer, // consumer
		false,       // auto-ack
		false,       // exclusive
		false,       // no-local
		false,       // no-wait
		nil,         // args
	)

	// todo: qos

	if err != nil {
		return nil, nil, err
	}

	return delivery, ch.close, nil
}

func (cn *Conn) Close() error {
	cn.muc.Lock()
	defer cn.muc.Unlock()

	for _, ch := range cn.channels {
		// stop consuming
		if ch.consumer != "" {
			ch.ch.Cancel(ch.consumer, false)
		}

		// close the channel
		ch.ch.Close()
	}

	return cn.conn.Close()
}

var wg sync.WaitGroup

func main() {
	go func() {
		cn, _ := NewConn("amqp://guest:guest@localhost:5673/", "default", time.Second)

		ch, _ := cn.Producer("default")

		for {
			delayMs := int64(10000)
			queueName := fmt.Sprintf(
				"delay.%d.%s.%s",
				delayMs, // delay duration in mileseconds
				"default",
				"default", // routing key
			)

			declareQueueArgs := amqp.Table{
				// Exchange where to send messages after TTL expiration.
				"x-dead-letter-exchange": "default",

				// Routing key which use when resending expired messages.
				"x-dead-letter-routing-key": "default",

				// Time in milliseconds
				// after that message will expire and be sent to destination.
				"x-message-ttl": delayMs,

				// Time after that the queue will be deleted.
				"x-expires": delayMs * 2,
			}

			log.Println(ch.QueueDeclare(
				queueName, // name
				true,      // type
				false,     // durable
				false,     // auto-deleted
				false,     // internal
				declareQueueArgs,
			))

			log.Println(ch.Publish(
				"",        // exchange
				queueName, // routing key
				false,     // mandatory
				false,     // immediate
				amqp.Publishing{
					ContentType:  "text/plain",
					Body:         []byte("DELAYED" + time.Now().String()),
					DeliveryMode: amqp.Persistent,
				},
			))
			//time.Sleep(time.Second)
		}
	}()

	cn, _ := NewConn("amqp://guest:guest@localhost:5673/", "default", time.Second)

	// go func() {
	// 	time.Sleep(time.Second * 5)
	// 	log.Println("stop request")
	// 	cn.channels["default"].ch.Cancel("my-consumer", false)
	// }()

	// time.Sleep(time.Second * 10)

	done := make(chan interface{})

	go func() {
		msgs, stop, _ := cn.Consumer("default")
		go consume(msgs, done)

		for {
			select {
			case err := <-stop:
				log.Println("consumer is stopped")

				if err == nil {
					log.Println("gracefully")
					close(done)
					return
				}

				select {
				case <-cn.retry:
					// reconnect
					nMsgs, nStop, err := cn.Consumer("default")
					if err != nil {
						close(done)
						return
					} else {
						go consume(nMsgs, done)
						stop = nStop
					}
				}
			}
		}
	}()

	log.Println("working")
	<-done
	log.Println("waiting")
	wg.Wait()
	log.Println("done")
}

func consume(delivery <-chan amqp.Delivery, done chan interface{}) {
	for d := range delivery {
		wg.Add(1)

		go func(d amqp.Delivery) {
			defer wg.Done()

			// 			time.Sleep(time.Second)
			err := d.Ack(false)
			if err != nil {
				log.Printf("ERRO %s", err)
			} else {
				log.Printf("DONE %s", d.Body)
			}
		}(d)
	}
}
