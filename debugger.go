package jobs

import (
	"github.com/sirupsen/logrus"
	"github.com/spiral/roadrunner"
	"github.com/spiral/roadrunner/cmd/rr/utils"
	"strings"
)

// Listener creates new debug listener.
func Listener(logger *logrus.Logger) func(event int, ctx interface{}) {
	return (&debugger{logger}).listener
}

// listener provide debug callback for system events. With colors!
type debugger struct{ logger *logrus.Logger }

// listener listens to http events and generates nice looking output.
func (s *debugger) listener(event int, ctx interface{}) {
	switch event {
	case roadrunner.EventWorkerKill:
		w := ctx.(*roadrunner.Worker)
		s.logger.Warning(utils.Sprintf(
			"<white+hb>worker.%v</reset> <yellow>killed</red>",
			*w.Pid,
		))
	case roadrunner.EventWorkerError:
		err := ctx.(roadrunner.WorkerError)
		s.logger.Error(utils.Sprintf(
			"<white+hb>worker.%v</reset> <red>%s</reset>",
			*err.Worker.Pid,
			err.Caused,
		))
	}

	// outputs
	switch event {
	case roadrunner.EventStderrOutput:
		s.logger.Warning(utils.Sprintf(
			"<yellow>%s</reset>",
			strings.Trim(string(ctx.([]byte)), "\r\n"),
		))
	}

	// rr server events
	switch event {
	case roadrunner.EventServerFailure:
		s.logger.Error(utils.Sprintf("<red>server is dead</reset>"))
	}

	// pool events
	switch event {
	case roadrunner.EventPoolConstruct:
		s.logger.Debug(utils.Sprintf("<cyan>new worker pool</reset>"))
	case roadrunner.EventPoolError:
		s.logger.Error(utils.Sprintf("<red>%s</reset>", ctx))
	}

	//s.logger.Warning(event, ctx)
}
