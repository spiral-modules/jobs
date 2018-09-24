package sqs

import (
	"errors"
	"github.com/spiral/jobs"
	"github.com/aws/aws-sdk-go/aws"
)

// Queue defines single SQS queue.
type Queue struct {
	// Queue is queue name.
	Queue string

	// URL is queue url.
	URL *string

	// Create indicates that queue must be automatically created.
	Create bool

	// Indicates that tube must be listened.
	Listen bool

	// Timeout - The duration (in seconds) that the received messages are hidden from subsequent. Default 600.
	Timeout int

	// WaitTime defines the number of seconds queue waits for job to arrive. Default 1.
	WaitTime int

	// Number of threads to serve tube with.
	Threads int
}

// CreateAttributes must return queue create attributes.
func (q *Queue) CreateAttributes() map[string]*string {
	// todo: add more attributes
	return map[string]*string{
		"MessageRetentionPeriod": aws.String("86400"),
	}
}

// NewTube creates new tube or returns an error
func NewQueue(p *jobs.Pipeline) (*Queue, error) {
	if p.Options.String("queue", "") == "" {
		return nil, errors.New("missing `queue` parameter on sqs pipeline")
	}

	if p.Options.Integer("threads", 1) < 1 {
		return nil, errors.New("invalid `threads` value for sqs pipeline, must be 1 or greater")
	}

	return &Queue{
		Queue:    p.Options.String("queue", ""),
		Create:   p.Options.Bool("create", false),
		Listen:   p.Listen,
		Timeout:  p.Options.Integer("timeout", 600),
		WaitTime: p.Options.Integer("waitTime", 1),
		Threads:  p.Options.Integer("threads", 1),
	}, nil
}
