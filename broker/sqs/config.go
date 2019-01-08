package sqs

import (
	"errors"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/spiral/roadrunner/service"
	"time"
)

// Config defines sqs broker configuration.
type Config struct {
	// Region defined SQS region, not required when endpoint is not empty.
	Region string

	// Region defined AWS API key, not required when endpoint is not empty.
	Key string

	// Region defined AWS API secret, not required when endpoint is not empty.
	Secret string

	// Endpoint can be used to re-define SQS endpoint to custom location. Only for local development.
	Endpoint string

	// Prefetch number of jobs allowed to be fetched by each pipe at the same time. Default 2.
	Prefetch int

	// Reserve timeout in seconds. Default 1.
	Reserve int
}

// InitDefaults sets missing values to their default values.
func (c *Config) InitDefaults() error {
	c.Reserve = 1
	c.Prefetch = 4

	return nil
}

// Hydrate config values.
func (c *Config) Hydrate(cfg service.Config) error {
	if err := cfg.Unmarshal(c); err != nil {
		return err
	}

	if c.Region == "" {
		return errors.New("SQS region is missing")
	}

	if c.Key == "" {
		return errors.New("SQS key is missing")
	}

	if c.Secret == "" {
		return errors.New("SQS secret is missing")
	}

	return nil
}

// Session returns new AWS session.
func (c *Config) Session() (*session.Session, error) {
	return session.NewSession(&aws.Config{
		Region:      aws.String(c.Region),
		Credentials: credentials.NewStaticCredentials(c.Key, c.Secret, ""),
	})
}

// SQS returns new SQS instance or error.
func (c *Config) SQS() (*sqs.SQS, error) {
	sess, err := c.Session()
	if err != nil {
		return nil, err
	}

	if c.Endpoint == "" {
		return sqs.New(sess), nil
	}

	return sqs.New(sess, &aws.Config{Endpoint: aws.String(c.Endpoint)}), nil
}

// ReserveDuration returns number of seconds to reserve the job.
func (c *Config) ReserveDuration() time.Duration {
	return time.Duration(c.Reserve) * time.Second
}
