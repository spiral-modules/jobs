package beanstalk

import (
	"github.com/beanstalkd/go-beanstalk"
	"github.com/spiral/jobs/cpool"
	"strconv"
)

// jid converts job id into string.
func jid(id uint64) string {
	if id == 0 {
		return ""
	}
	return strconv.FormatUint(id, 10)
}

// wrapError into conn error when detected. softErr would not wrap any of no connection errors.
func wrapErr(err error, hide bool) error {
	if err == nil {
		return nil
	}

	if err, ok := err.(beanstalk.ConnError); ok {
		return cpool.ConnError{Err: err}
	}

	if cpool.IsConnError(err) {
		return cpool.ConnError{Err: err}
	}

	if hide {
		return nil
	}

	return err
}
