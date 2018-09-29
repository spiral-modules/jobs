package sqs

import (
	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/spiral/jobs"
	"sync"
	"strconv"
)

// Broker run jobs using Broker service.
type Broker struct {
	cfg         *Config
	mu          sync.Mutex
	stop        chan interface{}
	sqs         *sqs.SQS
	wg          sync.WaitGroup
	queue       map[*jobs.Pipeline]*Queue
	handlerPool chan jobs.Handler
	err         jobs.ErrorHandler
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

// Init configures local job broker.
func (b *Broker) Init(cfg *Config) (bool, error) {
	b.cfg = cfg
	return true, nil
}

// Serve tubes.
func (b *Broker) Serve() (err error) {
	b.sqs, err = b.cfg.SQS()
	if err != nil {
		return err
	}

	b.mu.Lock()
	b.stop = make(chan interface{})
	b.mu.Unlock()

	for _, q := range b.queue {
		if q.Create {
			if err := b.createQueue(q); err != nil {
				return err
			}
		}

		url, err := b.sqs.GetQueueUrl(&sqs.GetQueueUrlInput{
			QueueName: aws.String(q.Queue),
		})

		if err != nil {
			return err
		}

		q.URL = url.QueueUrl

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

// Push new job to queue
func (b *Broker) Push(p *jobs.Pipeline, j *jobs.Job) (string, error) {
	data, err := json.Marshal(j)
	if err != nil {
		return "", err
	}

	result, err := b.sqs.SendMessage(&sqs.SendMessageInput{
		DelaySeconds: aws.Int64(int64(j.Options.Delay)),
		MessageBody:  aws.String(string(data)),
		QueueUrl:     b.queue[p].URL,
	})

	if err != nil {
		return "", err
	}

	return *result.MessageId, nil
}

// Stat must fetch statistics about given pipeline or return error.
func (b *Broker) Stat(p *jobs.Pipeline) (stat *jobs.PipelineStat, err error) {
	r, err := b.sqs.GetQueueAttributes(&sqs.GetQueueAttributesInput{
		QueueUrl: b.queue[p].URL,
		AttributeNames: []*string{
			aws.String("ApproximateNumberOfMessages"),
			aws.String("ApproximateNumberOfMessagesDelayed"),
			aws.String("ApproximateNumberOfMessagesNotVisible"),
		},
	})

	stat = &jobs.PipelineStat{Pipeline: b.queue[p].Queue}

	for a, v := range r.Attributes {
		if a == "ApproximateNumberOfMessages" {
			if v, err := strconv.Atoi(*v); err == nil {
				stat.Pending = int64(v)
			}
		}

		if a == "ApproximateNumberOfMessagesNotVisible" {
			if v, err := strconv.Atoi(*v); err == nil {
				stat.Active = int64(v)
			}
		}

		if a == "ApproximateNumberOfMessagesDelayed" {
			if v, err := strconv.Atoi(*v); err == nil {
				stat.Delayed = int64(v)
			}
		}
	}

	return stat, nil
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

// createQueue creates sqs queue.
func (b *Broker) createQueue(q *Queue) error {
	_, err := b.sqs.CreateQueue(&sqs.CreateQueueInput{
		QueueName:  aws.String(q.Queue),
		Attributes: q.CreateAttributes(),
	})

	return err
}

// listen jobs from given tube
func (b *Broker) listen(q *Queue) {
	defer b.wg.Done()
	var job *jobs.Job
	var handler jobs.Handler
	for {
		select {
		case <-b.stop:
			return
		default:
			result, err := b.sqs.ReceiveMessage(&sqs.ReceiveMessageInput{
				QueueUrl:            q.URL,
				MaxNumberOfMessages: aws.Int64(1),
				VisibilityTimeout:   aws.Int64(int64(q.Timeout)),
				WaitTimeSeconds:     aws.Int64(int64(q.WaitTime)),
			})

			// todo: change visibility window if not ready yet

			if err != nil {
				// need additional logging
				continue
			}

			if len(result.Messages) == 0 {
				continue
			}

			err = json.Unmarshal([]byte(*result.Messages[0].Body), &job)
			if err != nil {
				// need additional logging
				continue
			}

			handler = <-b.handlerPool
			go func() {
				err = handler(*result.Messages[0].MessageId, job)
				b.handlerPool <- handler

				if err == nil {
					b.sqs.DeleteMessage(&sqs.DeleteMessageInput{
						QueueUrl:      q.URL,
						ReceiptHandle: result.Messages[0].ReceiptHandle,
					})
					return
				}

				if !job.CanRetry() {
					b.err(*result.Messages[0].MessageId, job, err)

					// todo: move to deleted ?

					b.sqs.DeleteMessage(&sqs.DeleteMessageInput{
						QueueUrl:      q.URL,
						ReceiptHandle: result.Messages[0].ReceiptHandle,
					})

					return
				}

				// request to return message back to query after some delay
				b.sqs.ChangeMessageVisibility(&sqs.ChangeMessageVisibilityInput{
					QueueUrl:          q.URL,
					ReceiptHandle:     result.Messages[0].ReceiptHandle,
					VisibilityTimeout: aws.Int64(int64(job.Options.RetryDelay)),
				})
			}()
		}
	}
}
