package amqp

import (
	"github.com/spiral/jobs"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestBroker_Consume_Job(t *testing.T) {
	b := &Broker{}
	b.Init(cfg)
	b.Register(pipe)

	exec := make(chan jobs.Handler, 1)
	assert.NoError(t, b.Consume(pipe, exec, func(id string, j *jobs.Job, err error) {}))

	go func() { assert.NoError(t, b.Serve()) }()
	time.Sleep(time.Millisecond * 500)
	defer b.Stop()

	jid, perr := b.Push(pipe, &jobs.Job{
		Job:     "test",
		Payload: "body",
		Options: &jobs.Options{},
	})

	assert.NotEqual(t, "", jid)
	assert.NoError(t, perr)

	waitJob := make(chan interface{})
	exec <- func(id string, j *jobs.Job) error {
		assert.Equal(t, jid, id)
		assert.Equal(t, "body", j.Payload)
		close(waitJob)
		return nil
	}

	<-waitJob
}
