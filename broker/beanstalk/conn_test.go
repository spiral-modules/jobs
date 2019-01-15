package beanstalk

import (
	"github.com/spiral/jobs"
	"github.com/stretchr/testify/assert"
	"io"
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

		go io.Copy(in, up)
		go io.Copy(up, in)

		p.mu.Lock()
		p.conn = append(p.conn, in, up)
		p.mu.Unlock()
	}
}

// wait for specific number of connections
func (p *tcpProxy) waitFor(count int) {
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

		time.Sleep(time.Millisecond)
	}
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

func TestBroker_Proxy_Test(t *testing.T) {
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
	proxy.waitFor(2)

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

func TestBroker_Proxy_StatInterrupted(t *testing.T) {
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

	_, err := b.Stat(pipe)
	assert.NoError(t, err)

	// break the connection and cause the reconnect
	proxy.waitFor(1)
	assert.Equal(t, 1, proxy.reset(false))

	_, err = b.Stat(pipe)
	assert.Error(t, err)

	proxy.waitFor(1)

	_, err = b.Stat(pipe)
	assert.NoError(t, err)
}
