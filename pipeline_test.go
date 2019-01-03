package jobs

import (
	"github.com/magiconair/properties/assert"
	"testing"
)

func TestConfig_Map(t *testing.T) {
	cfg := Pipeline{"options": map[string]interface{}{"ttl": 10}}

	assert.Equal(t, 10, cfg.Map("options").Integer("ttl", 0))
}
