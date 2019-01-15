package beanstalk

import (
	"github.com/spiral/jobs"
	"github.com/stretchr/testify/assert"
	"io"
	"log"
	"net"
	"sync"
	"testing"
	"time"
)

var (
	proxyCfg = &Config{
		Addr:    "tcp://localhost:11301",
		Timeout: 1,
	}

	proxy = &tcpProxy{
		listen:   "localhost:11301",
		upstream: "localhost:11300",
		accept:   true,
	}
)

type tcpProxy struct {
	listen   string
	upstream string
	mu       sync.Mutex
	accept   bool
	conn     []net.Conn
}

func (p *tcpProxy) serve() {
	l, err := net.Listen("tcp", p.listen)
	if err != nil {
		panic(err)
	}

	for {
		in, err := l.Accept()
		if err != nil {
			panic(err)
		}

		if !p.accepting() {
			in.Close()
		}

		up, err := net.Dial("tcp", p.upstream)
		if err != nil {
			panic(err)
		}

		log.Println("new conn")

		go io.Copy(in, up)
		go io.Copy(up, in)

		p.mu.Lock()
		p.conn = append(p.conn, in, up)
		p.mu.Unlock()
	}
}

// wait for specific number of connections
func (p *tcpProxy) waitConn(count int) *tcpProxy {
	p.mu.Lock()
	p.accept = true
	p.mu.Unlock()

	for {
		p.mu.Lock()
		current := len(p.conn)
		p.mu.Unlock()

		if current == count*2 {
			break
		}

		time.Sleep(time.Second)
	}

	return p
}

func (p *tcpProxy) reset(accept bool) int {
	p.mu.Lock()
	p.accept = accept
	defer p.mu.Unlock()

	count := 0
	for _, conn := range p.conn {
		conn.Close()
		count++
	}

	p.conn = nil
	return count / 2
}

func (p *tcpProxy) accepting() bool {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.accept
}

func init() {
	go proxy.serve()
}

func TestBroker_Durability_Clean(t *testing.T) {
	defer proxy.reset(true)

	b := &Broker{}
	b.Init(proxyCfg)
	b.Register(pipe)

	ready := make(chan interface{})
	b.Listen(func(event int, ctx interface{}) {
		if event == jobs.EventBrokerReady {
			close(ready)
		}
	})

	exec := make(chan jobs.Handler, 1)
	assert.NoError(t, b.Consume(pipe, exec, func(id string, j *jobs.Job, err error) {}))

	go func() { assert.NoError(t, b.Serve()) }()
	defer b.Stop()

	<-ready

	// expect 2 connections
	proxy.waitConn(2)

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

func TestBroker_Durability_Consume(t *testing.T) {
	defer proxy.reset(true)

	b := &Broker{}
	b.Init(proxyCfg)
	b.Register(pipe)

	ready := make(chan interface{})
	b.Listen(func(event int, ctx interface{}) {
		if event == jobs.EventBrokerReady {
			close(ready)
		}
	})

	exec := make(chan jobs.Handler, 1)
	assert.NoError(t, b.Consume(pipe, exec, func(id string, j *jobs.Job, err error) {}))

	go func() { assert.NoError(t, b.Serve()) }()
	defer b.Stop()

	<-ready

	proxy.waitConn(2).reset(false)

	jid, perr := b.Push(pipe, &jobs.Job{
		Job:     "test",
		Payload: "body",
		Options: &jobs.Options{},
	})

	assert.Error(t, perr)

	// restore
	proxy.waitConn(2)

	jid, perr = b.Push(pipe, &jobs.Job{
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

func TestBroker_Durability_Reschedule(t *testing.T) {
	defer proxy.reset(true)

	b := &Broker{}
	b.Init(proxyCfg)
	b.Register(pipe)

	ready := make(chan interface{})
	b.Listen(func(event int, ctx interface{}) {
		if event == jobs.EventBrokerReady {
			close(ready)
		} else {
			log.Println(ctx)
		}
	})

	exec := make(chan jobs.Handler, 1)
	assert.NoError(t, b.Consume(pipe, exec, func(id string, j *jobs.Job, err error) {}))

	go func() { assert.NoError(t, b.Serve()) }()
	defer b.Stop()

	<-ready

	proxy.waitConn(2)

	jid, perr := b.Push(pipe, &jobs.Job{
		Job:     "test",
		Payload: "body",
		Options: &jobs.Options{},
	})

	assert.NotEqual(t, "", jid)
	assert.NoError(t, perr)

	wg := sync.WaitGroup{}

	// expect job to come twice after the reconnect
	wg.Add(2)

	fired := false
	exec <- func(id string, j *jobs.Job) error {
		defer wg.Done()

		log.Println("got job", id)

		assert.Equal(t, jid, id)
		assert.Equal(t, "body", j.Payload)

		if !fired {
			fired = true

			// reset the connections
			proxy.reset(true)
		}

		return nil
	}

	wg.Wait()
}

func TestBroker_Durability_Stat(t *testing.T) {
	defer proxy.reset(true)

	b := &Broker{}
	b.Init(proxyCfg)
	b.Register(pipe)

	ready := make(chan interface{})
	b.Listen(func(event int, ctx interface{}) {
		if event == jobs.EventBrokerReady {
			close(ready)
		}
	})

	assert.NoError(t, b.Consume(pipe, nil, nil))

	go func() { assert.NoError(t, b.Serve()) }()
	defer b.Stop()

	<-ready
	proxy.waitConn(1)

	_, err := b.Stat(pipe)
	assert.NoError(t, err)

	proxy.reset(false)

	_, err = b.Stat(pipe)
	assert.Error(t, err)

	proxy.waitConn(1)

	_, err = b.Stat(pipe)
	assert.NoError(t, err)
}

func TestBroker_Durability_StopDead(t *testing.T) {
	defer proxy.reset(true)

	b := &Broker{}
	b.Init(proxyCfg)
	b.Register(pipe)

	ready := make(chan interface{})
	b.Listen(func(event int, ctx interface{}) {
		if event == jobs.EventBrokerReady {
			close(ready)
		}
	})

	exec := make(chan jobs.Handler, 1)
	assert.NoError(t, b.Consume(pipe, exec, func(id string, j *jobs.Job, err error) {}))

	go func() { assert.NoError(t, b.Serve()) }()

	<-ready

	proxy.waitConn(2).reset(false)

	b.Stop()
}
