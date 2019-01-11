package amqp

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/spiral/jobs"
	"github.com/streadway/amqp"
)

type Queue struct {
	Create     bool
	Listen     bool
	Queue      string
	Durable    bool
	AutoDelete bool
	Exclusive  bool
	NoWait     bool
	Arguments  amqp.Table

	Binds []Bind
}

type Bind struct {
	Key       string
	Exchange  string
	NoWait    bool
	Arguments amqp.Table
}

type BindsConfig map[interface{}]interface{}

// Bool must return option value as string or return default value.
func (o BindsConfig) Bool(name interface{}, d bool) bool {
	if value, ok := o[name]; ok {
		if b, ok := value.(bool); ok {
			return b
		}
	}

	return d
}

// String must return option value as string or return default value.
func (o BindsConfig) String(name interface{}, d string) string {
	if value, ok := o[name]; ok {
		if str, ok := value.(string); ok {
			return str
		}
	}

	return d
}

func NewQueue(p *jobs.Pipeline) (*Queue, error) {
	if p.Options.String("queue", "") == "" {
		return nil, errors.New("missing `queue` parameter on amqp pipeline")
	}

	q := &Queue{
		Listen:     p.Listen,
		Create:     p.Options.Bool("create", false),
		Queue:      p.Options.String("queue", ""),
		Durable:    p.Options.Bool("durable", false),
		AutoDelete: p.Options.Bool("auto_delete", false),
		Exclusive:  p.Options.Bool("exclusive", false),
		NoWait:     p.Options.Bool("no_wait", false),
	}

	if attrOptions, ok := p.Options["arguments"]; ok {
		if attributes, ok := attrOptions.(map[string]interface{}); ok {
			q.Arguments = attributes
		}
	}

	if bindsOptions, ok := p.Options["binds"]; ok {
		if binds, ok := bindsOptions.([]interface{}); ok {
			for _, bindConfig := range binds {
				interfaceValues := bindConfig.(map[interface{}]interface{})
				entity := BindsConfig{}

				for key, value := range interfaceValues {
					strKey := fmt.Sprintf("%v", key)

					entity[strKey] = value
				}

				if entity.String("exchange", "") == "" {
					return nil, errors.New("missing `exchange` parameter in amqp pipeline bind")
				}
				bind := Bind{
					Key:      entity.String("key", ""),
					Exchange: entity.String("exchange", ""),
					NoWait:   entity.Bool("nowait", false),
				}

				if attrOptions, ok := entity["arguments"]; ok {
					if attributes, ok := attrOptions.(map[string]interface{}); ok {
						bind.Arguments = attributes
					}
				}

				q.Binds = append(q.Binds, bind)
			}
		}
	}

	return q, nil
}

func (q *Queue) SendMessage(ch *amqp.Channel, job *jobs.Job) error {
	data, err := json.Marshal(job)
	if err != nil {
		return err
	}
	return ch.Publish(
		"",
		q.Queue,
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        data,
		})
}
