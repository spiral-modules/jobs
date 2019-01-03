package jobs

import "time"

// PipelineConfig defines pipeline config (
type Pipeline map[string]interface{}

// Broker associated with the pipeline.
func (c Pipeline) Broker() string {
	return c.String("broker", "")
}

// Bool must return nested map value or empty config.
func (c Pipeline) Map(name string) Pipeline {
	if value, ok := c[name]; ok {
		if m, ok := value.(map[string]interface{}); ok {
			return Pipeline(m)
		}
	}

	return Pipeline{}
}

// Bool must return option value as string or return default value.
func (c Pipeline) Bool(name string, d bool) bool {
	if value, ok := c[name]; ok {
		if b, ok := value.(bool); ok {
			return b
		}
	}

	return d
}

// String must return option value as string or return default value.
func (c Pipeline) String(name string, d string) string {
	if value, ok := c[name]; ok {
		if str, ok := value.(string); ok {
			return str
		}
	}

	return d
}

// Int must return option value as string or return default value.
func (c Pipeline) Integer(name string, d int) int {
	if value, ok := c[name]; ok {
		if str, ok := value.(int); ok {
			return str
		}
	}

	return d
}

// Duration must return option value as time.Duration (seconds) or return default value.
func (c Pipeline) Duration(name string, d time.Duration) time.Duration {
	if value, ok := c[name]; ok {
		if str, ok := value.(int); ok {
			return time.Second * time.Duration(str)
		}
	}

	return d
}
