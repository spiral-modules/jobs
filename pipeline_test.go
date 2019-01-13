package jobs

import (
	"github.com/magiconair/properties/assert"
	"testing"
	"time"
)

func TestPipeline_Map(t *testing.T) {
	pipe := Pipeline{"options": map[string]interface{}{"ttl": 10}}

	assert.Equal(t, 10, pipe.Map("options").Integer("ttl", 0))
	assert.Equal(t, 0, pipe.Map("other").Integer("ttl", 0))
}

func TestPipeline_Bool(t *testing.T) {
	pipe := Pipeline{"value": true}

	assert.Equal(t, true, pipe.Bool("value", false))
	assert.Equal(t, true, pipe.Bool("other", true))
}

func TestPipeline_String(t *testing.T) {
	pipe := Pipeline{"value": "value"}

	assert.Equal(t, "value", pipe.String("value", ""))
	assert.Equal(t, "value", pipe.String("other", "value"))
}

func TestPipeline_Integer(t *testing.T) {
	pipe := Pipeline{"value": 1}

	assert.Equal(t, 1, pipe.Integer("value", 0))
	assert.Equal(t, 1, pipe.Integer("other", 1))
}

func TestPipeline_Duration(t *testing.T) {
	pipe := Pipeline{"value": 1}

	assert.Equal(t, time.Second, pipe.Duration("value", 0))
	assert.Equal(t, time.Second, pipe.Duration("other", time.Second))
}

func TestPipeline_Has(t *testing.T) {
	pipe := Pipeline{"options": map[string]interface{}{"ttl": 10}}

	assert.Equal(t, true, pipe.Has("options"))
	assert.Equal(t, false, pipe.Has("other"))
}
