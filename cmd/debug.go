// Copyright (c) 2018 SpiralScout
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package cmd

import (
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spiral/jobs"
	rr "github.com/spiral/roadrunner/cmd/rr/cmd"
	"github.com/spiral/roadrunner/cmd/util"
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
	case jobs.EventPushComplete:
		e := ctx.(*jobs.JobEvent)
		s.logger.Info(util.Sprintf(
			"job.<magenta+h>PUSH</reset> <cyan>%s</reset> <white+hb>%s</reset>",
			e.Job.Job,
			e.ID,
		))

	case jobs.EventJobComplete:
		e := ctx.(*jobs.JobEvent)
		s.logger.Info(util.Sprintf(
			"job.<green+h>DONE</reset> <cyan>%s</reset> <white+hb>%s</reset>",
			e.Job.Job,
			e.ID,
		))

	case jobs.EventPushError:
		e := ctx.(*jobs.ErrorEvent)
		s.logger.Error(util.Sprintf(
			"job.<red>ERRO</reset> <cyan>%s</reset> <red+hb>%s</reset>",
			e.Job.Job,
			e.Error.Error(),
		))

	case jobs.EventJobError:
		e := ctx.(*jobs.ErrorEvent)
		s.logger.Error(util.Sprintf(
			"job.<red>ERRO</reset> <cyan>%s</reset> <white+hb>%s</reset> <yellow>%s</reset>",
			e.Job.Job,
			e.ID,
			e.Error.Error(),
		))
	}
}
