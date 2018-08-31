package main

import (
	rr "github.com/spiral/roadrunner/cmd/rr/cmd"
	"github.com/spiral/roadrunner/service/rpc"

	"github.com/sirupsen/logrus"

	"github.com/spiral/jobs"
	"github.com/spiral/jobs/broker"
)

func main() {
	rr.Container.Register(rpc.ID, &rpc.Service{})
	rr.Container.Register(jobs.ID, jobs.NewService(rr.Logger, map[string]jobs.Broker{
		"local": &broker.Local{},
		//"redis": &broker.Redis{},
	}))

	rr.Logger.Formatter = &logrus.TextFormatter{ForceColors: true}
	rr.Execute()
}