package sqs

import (
	"errors"
	"github.com/spiral/jobs"
)

// Queue defines single SQS queue.
type Queue struct {
	// Queue is queue name.
	Queue string

	// URL is queue url.
	URL *string

	// Create indicates that queue must be automatically created.
	Create bool

	// Attributes defines set of options to be used to create queue.
	Attributes map[interface{}]interface{}

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
func (q *Queue) CreateAttributes() (attr map[string]*string) {
	attr = make(map[string]*string)

	for k, v := range q.Attributes {
		if ks, ok := k.(string); ok {
			if vs, ok := v.(string); ok {
				attr[ks] = &vs
			}
		}
	}

	return attr
}

// NewTube creates new tube or returns an error
func NewQueue(p *jobs.Pipeline) (*Queue, error) {
	if p.Options.String("queue", "") == "" {
		return nil, errors.New("missing `queue` parameter on sqs pipeline")
	}

	if p.Options.Integer("threads", 1) < 1 {
		return nil, errors.New("invalid `threads` value for sqs pipeline, must be 1 or greater")
	}

	q := &Queue{
		Queue:    p.Options.String("queue", ""),
		Create:   p.Options.Bool("create", true),
		Listen:   p.Listen,
		Timeout:  p.Options.Integer("timeout", 600),
		WaitTime: p.Options.Integer("waitTime", 1),
		Threads:  p.Options.Integer("threads", 1),
	}

	if attrOptions, ok := p.Options["attributes"]; ok {
		if attributes, ok := attrOptions.(map[interface{}]interface{}); ok {
			q.Create = true
			q.Attributes = attributes
		}
	}

	return q, nil
}
