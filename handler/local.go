package handler

import (
	"github.com/spiral/roadrunner"
)

type Local struct {
	cfg  *roadrunner.ServerConfig
	rr   *roadrunner.Server
	done chan interface{}
}

func NewLocal(cfg *roadrunner.ServerConfig) (*Local, error) {
	return &Local{
		cfg: cfg,
	}, nil
}

func (l *Local) Handle() error {
	return nil
}

func (l *Local) Serve() error {
	l.rr = roadrunner.NewServer(l.cfg)
	if err := l.rr.Start(); err != nil {
		return err
	}

	l.done = make(chan interface{})
	<-l.done
	return nil
}

func (l *Local) Stop() {
	if l.done != nil {
		l.done <- nil
	}
}
