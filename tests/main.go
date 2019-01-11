package main

import (
	rr "github.com/spiral/roadrunner/cmd/rr/cmd"
	"github.com/spiral/roadrunner/service/rpc"

	"github.com/sirupsen/logrus"

	"github.com/spiral/jobs"
	"github.com/spiral/jobs/broker/amqp"
	"github.com/spiral/jobs/broker/beanstalk"
	"github.com/spiral/jobs/broker/local"
	"github.com/spiral/jobs/broker/sqs"
)

func main() {
	rr.Container.Register(rpc.ID, &rpc.Service{})
	rr.Container.Register(jobs.ID, &jobs.Service{
		Brokers: map[string]jobs.Broker{
			"local":     &local.Broker{},
			"beanstalk": &beanstalk.Broker{},
			"sqs":       &sqs.Broker{},
			"amqp":      &amqp.Broker{},
		},
	})

	rr.Logger.Formatter = &logrus.TextFormatter{ForceColors: true}
	rr.Execute()
}
