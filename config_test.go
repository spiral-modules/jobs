package jobs

import (
	"encoding/json"
	"github.com/spiral/roadrunner/service"
	"github.com/stretchr/testify/assert"
	"testing"
)

type mockCfg struct{ cfg string }

func (cfg *mockCfg) Get(name string) service.Config  { return nil }
func (cfg *mockCfg) Unmarshal(out interface{}) error { return json.Unmarshal([]byte(cfg.cfg), out) }

func Test_Config_Hydrate_Error1(t *testing.T) {
	cfg := &mockCfg{`{"enable": true}`}
	c := &Config{}

	assert.Error(t, c.Hydrate(cfg))
}

func Test_Config_Hydrate_Error2(t *testing.T) {
	cfg := &mockCfg{`{"dir": "/dir/"`}
	c := &Config{}

	assert.Error(t, c.Hydrate(cfg))
}

func Test_Config_Hydrate_Mapping(t *testing.T) {
	cfg := &mockCfg{`{
		"workers":{
			"pool":{
				"numWorkers": 1
			}
		},
		"mapping": {
			"default": ["some.*"],
			"other":   ["some.other.*"]
		}
	}`}
	c := &Config{}

	assert.NoError(t, c.Hydrate(cfg))
	assert.Equal(t, "default", c.Mapping.find("some"))
	assert.Equal(t, "default", c.Mapping.find("some.any"))
	assert.Equal(t, "other", c.Mapping.find("some.other"))
}

func Test_Config_Hydrate_Pipelines(t *testing.T) {
	cfg := &mockCfg{`{
		"workers":{
			"pool":{
				"numWorkers": 1
			}
		},
		"pipelines": [
			{
				"name":   "some",
				"broker": "local",
				"queue":  "default"
			}
		]
	}`}
	c := &Config{}

	assert.NoError(t, c.Hydrate(cfg))

	assert.Equal(t, "local", c.Pipelines.Get("some").Broker())
	assert.Equal(t, "default", c.Pipelines.Get("some").String("queue", ""))
	assert.Equal(t, "another-default", c.Pipelines.Get("some").String("another", "another-default"))
}
