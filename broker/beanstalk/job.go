package beanstalk

import (
	"bytes"
	"encoding/gob"
	"github.com/spiral/jobs/v2"
)

func pack(j *jobs.Job) []byte {
	b := new(bytes.Buffer)
	gob.NewEncoder(b).Encode(j)

	return b.Bytes()
}

func unpack(data []byte) (*jobs.Job, error) {
	j := &jobs.Job{}
	err := gob.NewDecoder(bytes.NewBuffer(data)).Decode(j)

	return j, err
}
