package beanstalk

import (
	"errors"
	"github.com/beanstalkd/go-beanstalk"
	"github.com/spiral/jobs"
	"github.com/spiral/jobs/cpool"
	"strconv"
	"sync"
	"time"
)

type tube struct {
	mut      sync.Mutex
	tube     *beanstalk.Tube
	connPool *cpool.ConnPool
	wait     chan interface{}
	execPool chan jobs.Handler
	err      jobs.ErrorHandler
}

func newTube(pipe *jobs.Pipeline, connPool *cpool.ConnPool) (*tube, error) {
	if pipe.String("tube", "") == "" {
		return nil, errors.New("missing `tube` parameter on beanstalk pipeline")
	}

	return &tube{tube: &beanstalk.Tube{Name: pipe.String("tube", "")}, connPool: connPool}, nil
}

func (t *tube) put(data []byte, attempt int, delay time.Duration, retry time.Duration) (id string, err error) {
	return "", nil
}

func (t *tube) stat() (stat *jobs.Stat, err error) {
	values := make(map[string]string)

	err = t.connPool.Exec(func(c interface{}) error {
		t.mut.Lock()
		defer t.mut.Unlock()

		t.tube.Conn = c.(*beanstalk.Conn)

		values, err = t.tube.Stats()
		return wrapErr(err, false)
	})

	if err != nil {
		return nil, err
	}

	stat = &jobs.Stat{InternalName: t.tube.Name}

	if v, err := strconv.Atoi(values["current-jobs-ready"]); err == nil {
		stat.Queue = int64(v)
	}

	if v, err := strconv.Atoi(values["current-jobs-reserved"]); err == nil {
		stat.Active = int64(v)
	}

	if v, err := strconv.Atoi(values["current-jobs-delayed"]); err == nil {
		stat.Delayed = int64(v)
	}

	return stat, err
}

func (t *tube) configure(execPool chan jobs.Handler, err jobs.ErrorHandler) error {
	if t.wait != nil {
		return errors.New("unable to configure active tube")
	}

	t.execPool = execPool
	t.err = err

	return nil
}

func (t *tube) serve() {

}

func (t *tube) stop() {

}
