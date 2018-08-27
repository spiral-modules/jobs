package main

import (

	rr "github.com/spiral/roadrunner/cmd/rr/cmd"

	"github.com/spiral/roadrunner/service/rpc"
	"github.com/spiral/jobs"
	"github.com/sirupsen/logrus"
)

func main() {
	rr.Container.Register(rpc.ID, &rpc.Service{})
	rr.Container.Register(jobs.ID, &jobs.Service{Logger: rr.Logger})

	rr.Logger.Formatter = &logrus.TextFormatter{ForceColors: true}
	rr.Execute()
}