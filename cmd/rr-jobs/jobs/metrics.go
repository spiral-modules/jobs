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

package jobs

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/spf13/cobra"
	"github.com/spiral/jobs"
	rr "github.com/spiral/roadrunner/cmd/rr/cmd"
	"github.com/spiral/roadrunner/service/metrics"
	"github.com/spiral/roadrunner/util"
	"time"
)

func init() {
	cobra.OnInitialize(func() {
		svc, _ := rr.Container.Get(metrics.ID)
		mtr, ok := svc.(*metrics.Service)
		if !ok || !mtr.Enabled() {
			return
		}

		ht, _ := rr.Container.Get(jobs.ID)
		if jbs, ok := ht.(*jobs.Service); ok {
			collector := newCollector()

			// register metrics
			mtr.MustRegister(collector.jobCounter)
			mtr.MustRegister(collector.jobDuration)
			mtr.MustRegister(collector.workersMemory)

			// collect events
			jbs.AddListener(collector.listener)

			// update memory usage every 10 seconds
			go collector.collectMemory(jbs, time.Second*10)
		}
	})
}

// listener provide debug callback for system events. With colors!
type metricCollector struct {
	jobCounter    *prometheus.CounterVec
	jobDuration   *prometheus.HistogramVec
	workersMemory prometheus.Gauge
}

func newCollector() *metricCollector {
	return &metricCollector{
		jobCounter: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "rr_job_total",
				Help: "Total number of handled jobs after server restart.",
			},
			[]string{"job", "ok"},
		),
		jobDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "rr_job_duration_seconds",
				Help: "Job execution duration.",
			},
			[]string{"job", "ok"},
		),
		workersMemory: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "rr_jobs_workers_memory_bytes",
				Help: "Memory usage by Jobs workers.",
			},
		),
	}
}

// listener listens to http events and generates nice looking output.
func (c *metricCollector) listener(event int, ctx interface{}) {
	switch event {
	case jobs.EventJobOK:
		e := ctx.(*jobs.JobEvent)

		c.jobCounter.With(prometheus.Labels{
			"job": e.Job.Job,
			"ok":  "true",
		}).Inc()

		c.jobDuration.With(prometheus.Labels{
			"job": e.Job.Job,
			"ok":  "true",
		}).Observe(e.Elapsed().Seconds())

	case jobs.EventJobError:
		e := ctx.(*jobs.JobError)

		c.jobCounter.With(prometheus.Labels{
			"job": e.Job.Job,
			"ok":  "false",
		}).Inc()

		c.jobDuration.With(prometheus.Labels{
			"job": e.Job.Job,
			"ok":  "false",
		}).Observe(e.Elapsed().Seconds())
	}
}

// collect memory usage by server workers
func (c *metricCollector) collectMemory(service *jobs.Service, tick time.Duration) {
	started := false
	for {
		server := service.Server()
		if server == nil && started {
			// stopped
			return
		}

		started = true

		if workers, err := util.ServerState(server); err == nil {
			sum := 0.0
			for _, w := range workers {
				sum = sum + float64(w.MemoryUsage)
			}

			c.workersMemory.Set(sum)
		}

		time.Sleep(tick)
	}
}
