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
	"fmt"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spiral/jobs"
	rr "github.com/spiral/roadrunner/cmd/rr/cmd"
	"github.com/spiral/roadrunner/cmd/util"
	"time"
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
			"job.<green+h>DONE</reset> <cyan>%s</reset> %s <white+hb>%s</reset>",
			e.Job.Job,
			elapsed(e.Elapsed()),
			e.ID,
		))

	case jobs.EventJobError:
		e := ctx.(*jobs.JobError)
		s.logger.Error(util.Sprintf(
			"job.<red>ERRO</reset> <cyan>%s</reset> <white+hb>%s</reset> <yellow>%s</reset>",
			e.Job.Job,
			e.ID,
			e.Error(),
		))

	case jobs.EventPushError:
		e := ctx.(*jobs.JobError)
		s.logger.Error(util.Sprintf(
			"job.<red>ERRO</reset> <cyan>%s</reset> <red+hb>%s</reset>",
			e.Job.Job,
			e.Error(),
		))

	case jobs.EventPipelineConsume:
		e := ctx.(*jobs.Pipeline)
		s.logger.Info(util.Sprintf(
			"[%s]: resuming {<green+hb>%s</reset>}",
			e.Broker(),
			e.Name(),
		))

	case jobs.EventPipelineStop:
		e := ctx.(*jobs.Pipeline)
		s.logger.Info(util.Sprintf(
			"[%s]: stopping {<yellow+hb>%s</reset>}",
			e.Broker(),
			e.Name(),
		))

	case jobs.EventPipelineError:
		e := ctx.(*jobs.PipelineError)
		s.logger.Error(util.Sprintf(
			"[%s]: <red>{%s}</reset> <red+hb>%s</reset>",
			e.Pipeline.Broker(),
			e.Pipeline.Name(),
			e.Error(),
		))
	}
}

// fits duration into 5 characters
func elapsed(d time.Duration) string {
	var v string
	switch {
	case d > 100*time.Second:
		v = fmt.Sprintf("%.1fs", d.Seconds())
	case d > 10*time.Second:
		v = fmt.Sprintf("%.2fs", d.Seconds())
	case d > time.Second:
		v = fmt.Sprintf("%.3fs", d.Seconds())
	case d > 100*time.Millisecond:
		v = fmt.Sprintf("%.0fms", d.Seconds()*1000)
	case d > 10*time.Millisecond:
		v = fmt.Sprintf("%.1fms", d.Seconds()*1000)
	default:
		v = fmt.Sprintf("%.2fms", d.Seconds()*1000)
	}

	if d > time.Second {
		return util.Sprintf("<red>{%v}</reset>", v)
	}

	if d > time.Millisecond*500 {
		return util.Sprintf("<yellow>{%v}</reset>", v)
	}

	return util.Sprintf("<gray+hb>{%v}</reset>", v)
}
