package main

import (
	rr "github.com/spiral/roadrunner/cmd/rr/cmd"
	"github.com/spiral/roadrunner/service/rpc"

	"github.com/sirupsen/logrus"

	"github.com/spiral/jobs"
	"github.com/spiral/jobs/endpoint"
)

func main() {
	rr.Container.Register(rpc.ID, &rpc.Service{})
	rr.Container.Register(jobs.ID, jobs.NewService(rr.Logger, map[string]jobs.Endpoint{
		"local": &endpoint.Local{},
		"redis": &endpoint.Redis{},
	}))

	rr.Logger.Formatter = &logrus.TextFormatter{ForceColors: true}
	rr.Execute()
}
