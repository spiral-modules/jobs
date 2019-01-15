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
	}
)

type tcpProxy struct {
	listen   string
	upstream string
	mu       sync.Mutex
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

		up, err := net.Dial("tcp", p.upstream)
		if err != nil {
			panic(err)
		}

		go p.pipe(in, up)
		go p.pipe(up, in)

		p.mu.Lock()
		p.conn = append(p.conn, in, up)
		p.mu.Unlock()
	}
}

func (p *tcpProxy) pipe(src, dst io.ReadWriter) {
	buff := make([]byte, 0xffff)
	for {
		n, err := src.Read(buff)
		if err != nil {
			return
		}
		b := buff[:n]

		// write out result
		n, err = dst.Write(b)
		if err != nil {
			panic(err)
			return
		}
	}
}

// wait for specific number of connections
func (p *tcpProxy) waitFor(count int) {
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

func (p *tcpProxy) reset() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	count := 0
	for _, conn := range p.conn {
		conn.Close()
		count++
	}

	p.conn = nil
	return count / 2
}

func init() {
	go proxy.serve()
}

func TestBroker_Proxy_Test(t *testing.T) {
	defer proxy.reset()

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

func TestBroker_Proxy_PushInterrupted(t *testing.T) {
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

	// break the connection and cause the reconnect
	proxy.waitFor(1)
	assert.Equal(t, 1, proxy.reset())

	_, perr := b.Push(pipe, &jobs.Job{
		Job:     "test",
		Payload: "body",
		Options: &jobs.Options{},
	})

	assert.Error(t, perr)
}

// func TestBroker_Proxy_ConsumeInterrupted(t *testing.T) {
// 	tx.CreateProxy("beanstalk", "localhost:11301", "localhost:11300")
// 	defer tx.ResetState()
//
// 	b := &Broker{}
// 	b.Init(proxyCfg)
// 	b.Register(pipe)
//
// 	ready := make(chan interface{})
// 	b.Listen(func(event int, ctx interface{}) {
// 		if event == jobs.EventBrokerReady {
// 			reset(ready)
// 		}
// 	})
//
// 	exec := make(chan jobs.Handler, 1)
// 	assert.NoError(t, b.Consume(pipe, exec, func(id string, j *jobs.Job, err error) {}))
//
// 	go func() { assert.NoError(t, b.Serve()) }()
// 	defer b.Stop()
//
// 	<-ready
//
// 	jid, perr := b.Push(pipe, &jobs.Job{
// 		Job:     "test",
// 		Payload: "body",
// 		Options: &jobs.Options{},
// 	})
//
// 	assert.NotEqual(t, "", jid)
// 	assert.NoError(t, perr)
//
// 	// break the connection and cause the reconnect
// 	tx.ResetState()
// 	tx.CreateProxy("beanstalk", "localhost:11301", "localhost:11300")
//
// 	waitJob := make(chan interface{})
// 	exec <- func(id string, j *jobs.Job) error {
// 		assert.Equal(t, jid, id)
// 		assert.Equal(t, "body", j.Payload)
// 		reset(waitJob)
// 		return nil
// 	}
//
// 	<-waitJob
// }
