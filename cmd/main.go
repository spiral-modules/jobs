package main

import (
	// colorful logging
	"github.com/sirupsen/logrus"

	rr "github.com/spiral/roadrunner/cmd/rr/cmd"

	"github.com/spiral/roadrunner/service/rpc"
	"github.com/spiral/jobs"
)

func main() {
	rr.Logger.Formatter = &logrus.TextFormatter{ForceColors: true}

	rr.Container.Register(rpc.ID, &rpc.Service{})
	rr.Container.Register(jobs.ID, &jobs.Service{Logger:rr.Logger})

	// you can register additional commands using cmd.CLI
	rr.Execute()
}
