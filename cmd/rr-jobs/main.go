package main

import (
	"github.com/sirupsen/logrus"
	"github.com/spiral/jobs"
	"github.com/spiral/jobs/broker/beanstalk"
	"github.com/spiral/jobs/broker/local"
	"github.com/spiral/roadrunner/service/rpc"

	_ "github.com/spiral/jobs/cmd/rr-jobs/cmd"
	rr "github.com/spiral/roadrunner/cmd/rr/cmd"
)

func main() {
	rr.Container.Register(rpc.ID, &rpc.Service{})
	rr.Container.Register(jobs.ID, &jobs.Service{
		Brokers: map[string]jobs.Broker{
			"local":     &local.Broker{},
			"beanstalk": &beanstalk.Broker{},
		},
	})

	rr.Logger.Formatter = &logrus.TextFormatter{ForceColors: true}
	rr.Execute()
}
