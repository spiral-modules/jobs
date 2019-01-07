package cpool

import (
	"github.com/magiconair/properties/assert"
	"github.com/pkg/errors"
	"testing"
)

func TestIsConnError(t *testing.T) {
	ce := ConnError{Caused: errors.New("error")}
	assert.Equal(t, "error", ce.Error())
}
