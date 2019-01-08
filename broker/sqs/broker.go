package sqs

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/spiral/jobs"
	"strconv"
	"sync"
)

// Broker represents SQS broker.
type Broker struct {
	cfg    *Config
	sqs    *sqs.SQS
	lsns   []func(event int, ctx interface{})
	mu     sync.Mutex
	wait   chan error
	queues map[*jobs.Pipeline]*queue
}

// AddListener attaches server event watcher.
func (b *Broker) AddListener(l func(event int, ctx interface{})) {
	b.lsns = append(b.lsns, l)
}

// Start configures local job broker.
func (b *Broker) Init(cfg *Config) (ok bool, err error) {
	b.cfg = cfg
	b.sqs, err = b.cfg.SQS()
	if err != nil {
		return false, err
	}

	b.queues = make(map[*jobs.Pipeline]*queue)

	return true, nil
}

// Register broker pipeline.
func (b *Broker) Register(pipe *jobs.Pipeline) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if pipe.String("queue", "") == "" {
		return errors.New("missing `queue` parameter on sqs pipeline")
	}

	url, err := b.ensureQueue(pipe)
	if err != nil {
		return err
	}

	q, err := newQueue(
		pipe,
		b.sqs,
		url,
		b.cfg.ReserveDuration(),
		b.throw,
	)

	if err != nil {
		return err
	}

	b.queues[pipe] = q

	return nil
}

// Serve broker pipelines.
func (b *Broker) Serve() (err error) {
	b.mu.Lock()
	for _, q := range b.queues {
		go q.serve(b.cfg.Prefetch)
	}
	b.wait = make(chan error)
	b.mu.Unlock()

	return <-b.wait
}

// Stop all pipelines.
func (b *Broker) Stop() {
	b.stopError(nil)
}

// Consume configures pipeline to be consumed. Set execPool to nil to disable consuming. Method can be called before
// the service is started!
func (b *Broker) Consume(pipe *jobs.Pipeline, execPool chan jobs.Handler, errHandler jobs.ErrorHandler) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	q, ok := b.queues[pipe]
	if !ok {
		return errors.New("invalid pipeline")
	}

	q.stop()

	if err := q.configure(execPool, errHandler); err != nil {
		return err
	}

	if b.wait != nil && q.execPool != nil {
		// resume wg
		go q.serve(b.cfg.Prefetch)
	}

	return nil
}

// Push job into the worker.
func (b *Broker) Push(pipe *jobs.Pipeline, j *jobs.Job) (string, error) {
	if err := b.isServing(); err != nil {
		return "", err
	}

	if j.Options.Delay > 900 {
		return "", fmt.Errorf(
			"unable to push into `%s`, got delay %v (maximum 900)",
			pipe.Name(),
			j.Options.Delay,
		)
	}

	if j.Options.RetryDelay > 900 {
		return "", fmt.Errorf(
			"unable to push into `%s`, got retry delay %v (maximum 900)",
			pipe.Name(),
			j.Options.RetryDelay,
		)
	}

	t := b.queue(pipe)
	if t == nil {
		return "", fmt.Errorf("undefined queue `%s`", pipe.Name())
	}

	data, err := json.Marshal(j)
	if err != nil {
		return "", err
	}

	return t.send(data, j.Options.DelayDuration(), j.Options.RetryDuration())
}

// Stat must fetch statistics about given pipeline or return error.
func (b *Broker) Stat(pipe *jobs.Pipeline) (stat *jobs.Stat, err error) {
	if err := b.isServing(); err != nil {
		return nil, err
	}

	t := b.queue(pipe)
	if t == nil {
		return nil, fmt.Errorf("undefined queue `%s`", pipe.Name())
	}

	return t.stat()
}

// // createQueue creates sqs queue.
func (b *Broker) ensureQueue(pipe *jobs.Pipeline) (*string, error) {
	attr := make(map[string]*string)
	for k, v := range pipe.Map("options") {
		if vs, ok := v.(string); ok {
			attr[k] = aws.String(vs)
		}

		if vi, ok := v.(int); ok {
			attr[k] = aws.String(strconv.Itoa(vi))
		}
	}

	if len(attr) != 0 || pipe.Bool("create", false) {
		res, err := b.sqs.CreateQueue(&sqs.CreateQueueInput{
			QueueName:  aws.String(pipe.String("queue", "")),
			Attributes: attr,
		})

		return res.QueueUrl, err
	}

	// no need to create (get existed)
	url, err := b.sqs.GetQueueUrl(&sqs.GetQueueUrlInput{
		QueueName: aws.String(pipe.String("queue", "")),
	})

	if err != nil {
		return nil, err
	}

	return url.QueueUrl, nil
}

// queue returns queue associated with the pipeline.
func (b *Broker) queue(pipe *jobs.Pipeline) *queue {
	b.mu.Lock()
	defer b.mu.Unlock()

	q, ok := b.queues[pipe]
	if !ok {
		return nil
	}

	return q
}

// stop broker and send error
func (b *Broker) stopError(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.wait == nil {
		return
	}

	for _, q := range b.queues {
		q.stop()
	}

	b.wait <- err
}

// check if broker is serving
func (b *Broker) isServing() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.wait == nil {
		return errors.New("broker is not running")
	}

	return nil
}

// throw handles service, server and pool events.
func (b *Broker) throw(event int, ctx interface{}) {
	for _, l := range b.lsns {
		l(event, ctx)
	}
}
