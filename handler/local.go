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
		ID  string `json:"id"`
		Job string `json:"job"`
	}{
		ID:  id.String(),
		Job: j.Job,
	})
	if err != nil {
		return "", err
	}

	l.log.Debugf("[jobs.local] %s registered", id)

	var delay time.Duration
	if j.Options.Delay != nil {
		delay = time.Second * time.Duration(*j.Options.Delay)
	}

	go l.runJob(delay, payload, context, id.String())

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

func (l *local) runJob(delay time.Duration, data, ctx []byte, id string) {
	if delay != 0 {
		time.Sleep(delay)
	}

	if _, err := l.rr.Exec(&roadrunner.Payload{Body: data, Context: ctx}); err != nil {
		l.log.Errorf("[jobs.local] %s: %s", id, err)
	} else {
		l.log.Debugf("[jobs.local] %s complete", id)
	}
}
