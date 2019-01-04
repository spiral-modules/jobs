package jobs

import (
	"fmt"
	"time"
)

// Pipelines is list of Pipeline.
type Pipelines []*Pipeline

// Valid validates given list of pipelines.
func (ps Pipelines) Valid() error {
	for _, p := range ps {
		if p.Name() == "" {
			return fmt.Errorf("found unnamed pipeline")
		}

		if p.Broker() == "" {
			return fmt.Errorf("found the pipeline without defined broker")
		}
	}

	return nil
}

// Reverse returns pipelines in reversed order.
func (ps Pipelines) Reverse() Pipelines {
	out := make(Pipelines, len(ps))

	for i, p := range ps {
		out[len(ps)-i-1] = p
	}

	return out
}

// Filter return pipelines associated with specific broker.
func (ps Pipelines) Broker(broker string) Pipelines {
	out := make(Pipelines, 0)

	for _, p := range ps {
		if p.Broker() != broker {
			continue
		}

		out = append(out, p)
	}

	return out
}

// Names returns only pipelines with specified names.
func (ps Pipelines) Names(only ...string) Pipelines {
	out := make(Pipelines, 0)

	for _, name := range only {
		for _, p := range ps {
			if p.Name() == name {
				out = append(out, p)
			}
		}
	}

	return out
}

// Get returns pipeline by it's name.
func (ps Pipelines) Get(name string) *Pipeline {
	for _, p := range ps {
		if p.Name() == name {
			return p
		}
	}

	return nil
}

// PipelineConfig defines pipeline options.
type Pipeline map[string]interface{}

// Name returns pipeline name.
func (c Pipeline) Name() string {
	return c.String("name", "")
}

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
