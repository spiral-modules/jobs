package main

import (
	"github.com/spiral/jobs/v2"
	"github.com/spiral/jobs/v2/broker/amqp"
	"github.com/spiral/jobs/v2/broker/beanstalk"
	"github.com/spiral/jobs/v2/broker/ephemeral"
	"github.com/spiral/jobs/v2/broker/sqs"

	rr "github.com/spiral/roadrunner/cmd/rr/cmd"
	"github.com/spiral/roadrunner/service/limit"
	"github.com/spiral/roadrunner/service/metrics"
	"github.com/spiral/roadrunner/service/rpc"

	_ "github.com/spiral/jobs/v2/cmd/rr-jobs/jobs"
)

func main() {
	rr.Container.Register(rpc.ID, &rpc.Service{})
	rr.Container.Register(jobs.ID, &jobs.Service{
		Brokers: map[string]jobs.Broker{
			"amqp":      &amqp.Broker{},
			"ephemeral": &ephemeral.Broker{},
			"beanstalk": &beanstalk.Broker{},
			"sqs":       &sqs.Broker{},
		},
	})

	rr.Container.Register(metrics.ID, &metrics.Service{})
	rr.Container.Register(limit.ID, &limit.Service{})

	rr.Execute()
}
