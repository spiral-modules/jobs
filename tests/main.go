package main

import (
	rr "github.com/spiral/roadrunner/cmd/rr/cmd"
	"github.com/spiral/roadrunner/service/rpc"

	"github.com/sirupsen/logrus"

	"github.com/spiral/jobs"
	"github.com/spiral/jobs/broker/local"
)

func main() {
	rr.Container.Register(rpc.ID, &rpc.Service{})
	rr.Container.Register(jobs.ID, &jobs.Service{
		Brokers: map[string]jobs.Broker{
			"local": &local.Broker{},
		},
	})

	rr.Logger.Formatter = &logrus.TextFormatter{ForceColors: true}
	rr.Execute()
}
