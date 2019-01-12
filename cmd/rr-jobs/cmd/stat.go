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
	tm "github.com/buger/goterm"
	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
	"github.com/spf13/cobra"
	"github.com/spiral/jobs"
	rr "github.com/spiral/roadrunner/cmd/rr/cmd"
	"github.com/spiral/roadrunner/cmd/util"
	"net/rpc"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func init() {
	statsCommand := &cobra.Command{
		Use:   "jobs:stat",
		Short: "List all job pipeline stats",
		RunE:  statsCommand,
	}

	statsCommand.Flags().BoolVarP(
		&interactive,
		"interactive",
		"i",
		false,
		"render interactive pipeline table",
	)

	rr.CLI.AddCommand(statsCommand)

	signal.Notify(stopSignal, syscall.SIGTERM)
	signal.Notify(stopSignal, syscall.SIGINT)
}

func statsCommand(cmd *cobra.Command, args []string) (err error) {
	defer func() {
		if r, ok := recover().(error); ok {
			err = r
		}
	}()

	client, err := util.RPCClient(rr.Container)
	if err != nil {
		return err
	}
	defer client.Close()

	if !interactive {
		showStats(client)
		return nil
	}

	tm.Clear()
	for {
		select {
		case <-stopSignal:
			return nil
		case <-time.NewTicker(time.Millisecond * 500).C:
			tm.MoveCursor(1, 1)
			showStats(client)
			tm.Flush()
		}
	}
}

func showStats(client *rpc.Client) {
	var s jobs.PipelineList
	if err := client.Call("jobs.Stat", true, &s); err != nil {
		// skip errors
		return
	}

	StatTable(s.Pipelines).Render()
}

// StatTable renders table with information about all active pipelines.
func StatTable(pipelines []*jobs.Stat) *tablewriter.Table {
	tw := tablewriter.NewWriter(os.Stdout)
	tw.SetHeader([]string{"Pipeline", "Broker", "Name", "Queue", "Delayed", "Active"})

	for _, p := range pipelines {
		tw.Append([]string{
			util.Sprintf("<cyan>%s</reset>", p.Pipeline),
			util.Sprintf("<white+hb>%s</reset>", p.Broker),
			util.Sprintf("<gray+hb>%s</reset>", p.InternalName),
			util.Sprintf("<magenta>%s</reset>", humanize.Comma(p.Queue)),
			util.Sprintf("<yellow>%s</reset>", humanize.Comma(p.Delayed)),
			util.Sprintf("<green>%s</reset>", humanize.Comma(p.Active)),
		})
	}

	return tw
}
