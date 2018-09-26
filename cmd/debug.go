package cmd

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	rr "github.com/spiral/roadrunner/cmd/rr/cmd"
	"github.com/spiral/roadrunner/cmd/util"
	"github.com/spiral/jobs"
)

func init() {
	cobra.OnInitialize(func() {
		if rr.Debug {
			svc, _ := rr.Container.Get(jobs.ID)
			if svc, ok := svc.(*jobs.Service); ok {
				svc.AddListener((&debugger{logger: rr.Logger}).listener)
			}
		}
	})
}

// listener provide debug callback for system events. With colors!
type debugger struct{ logger *logrus.Logger }

// listener listens to http events and generates nice looking output.
func (s *debugger) listener(event int, ctx interface{}) {
	if util.LogEvent(s.logger, event, ctx) {
		// handler by default debug package
		return
	}

	switch event {
	case jobs.EventJobAdded:
		e := ctx.(*jobs.JobEvent)
		s.logger.Info(util.Sprintf(
			"jobs.<magenta+h>PUSH</reset> <cyan>%s</reset> <white+hb>%s</reset>",
			e.Job.Job,
			e.ID,
		))
	case jobs.EventJobComplete:
		e := ctx.(*jobs.JobEvent)
		s.logger.Info(util.Sprintf(
			"jobs.<green+h>DONE</reset> <cyan>%s</reset> <white+hb>%s</reset>",
			e.Job.Job,
			e.ID,
		))
	case jobs.EventPushError:
		e := ctx.(*jobs.ErrorEvent)
		s.logger.Error(util.Sprintf(
			"jobs.<red>ERRO</reset> <cyan>%s</reset> <red+hb>%s</reset>",
			e.Job.Job,
			e.Error.Error(),
		))

	case jobs.EventJobError:
		e := ctx.(*jobs.ErrorEvent)
		s.logger.Error(util.Sprintf(
			"jobs.<red>ERRO</reset> <cyan>%s</reset> <white+hb>%s</reset> <yellow>%s</reset>",
			e.Job.Job,
			e.ID,
			e.Error.Error(),
		))
	}
}
