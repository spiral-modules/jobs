package handler

import (
	"github.com/spiral/roadrunner"
	"github.com/satori/go.uuid"
	"github.com/sirupsen/logrus"
	"time"
	"github.com/spiral/roadrunner/cmd/rr/debug"
	"github.com/gin-gonic/gin/json"
)

type local struct {
	log  *logrus.Logger
	cfg  *roadrunner.ServerConfig
	rr   *roadrunner.Server
	done chan interface{}
}

func LocalHandler(cfg *roadrunner.ServerConfig, logger *logrus.Logger) (*local, error) {
	return &local{log: logger, cfg: cfg}, nil
}

func (l *local) Handle(j *Job) (string, error) {
	id, err := uuid.NewV4()
	if err != nil {
		return "", err
	}

	payload, err := json.Marshal(j.Payload)
	if err != nil {
		return "", err
	}

	context, err := json.Marshal(struct {
		Job string
	}{
		Job: j.Job,
	})
	if err != nil {
		return "", err
	}
	l.log.Debugf("[jobs.local] %s registered", id)

	go func(payload, context []byte, id string) {
		if j.Options.Delay != nil {
			time.Sleep(time.Second * time.Duration(*j.Options.Delay))
		}

		if _, err := l.rr.Exec(&roadrunner.Payload{Body: payload, Context: context,}); err != nil {
			l.log.Errorf("[jobs.local] %s: %s", id, err)
		} else {
			l.log.Debugf("[jobs.local] %s complete", id)
		}
	}(payload, context, id.String())

	return id.String(), nil
}

func (l *local) Serve() error {
	l.rr = roadrunner.NewServer(l.cfg)
	l.rr.Listen(debug.Listener(l.log))

	if err := l.rr.Start(); err != nil {
		return err
	}

	l.done = make(chan interface{})
	<-l.done
	return nil
}

func (l *local) Stop() {
	if l.done != nil {
		l.done <- nil
	}
}
