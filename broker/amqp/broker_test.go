package amqp

import (
	"github.com/spiral/jobs"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

var (
	pipe = &jobs.Pipeline{
		"broker": "amqp",
		"name":   "default",
		"queue":  "default",
	}

	cfg = &Config{
		Addr: "amqp://guest:guest@localhost:5672/",
	}
)

func TestBroker_Init(t *testing.T) {
	b := &Broker{}
	ok, err := b.Init(cfg)
	assert.True(t, ok)
	assert.NoError(t, err)
}

func TestBroker_Register(t *testing.T) {
	b := &Broker{}
	b.Init(cfg)
	assert.NoError(t, b.Register(pipe))
}

func TestBroker_Register_Twice(t *testing.T) {
	b := &Broker{}
	b.Init(cfg)
	assert.NoError(t, b.Register(pipe))
	assert.Error(t, b.Register(pipe))
}

func TestBroker_Consume_Nil_BeforeServe(t *testing.T) {
	b := &Broker{}
	b.Init(cfg)
	b.Register(pipe)
	assert.NoError(t, b.Consume(pipe, nil, nil))
}

func TestBroker_Consume_BeforeServe(t *testing.T) {
	b := &Broker{}
	b.Init(cfg)
	b.Register(pipe)

	exec := make(chan jobs.Handler)
	err := func(id string, j *jobs.Job, err error) {}

	assert.NoError(t, b.Consume(pipe, exec, err))
}

func TestBroker_Consume_Serve_Nil_Stop(t *testing.T) {
	b := &Broker{}
	b.Init(cfg)
	b.Register(pipe)

	b.Consume(pipe, nil, nil)

	wait := make(chan interface{})
	go func() {
		assert.NoError(t, b.Serve())
		close(wait)
	}()
	time.Sleep(time.Millisecond * 100)
	b.Stop()

	<-wait
}
