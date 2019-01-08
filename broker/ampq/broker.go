package ampq

// Broker represents AMPQ broker.
type Broker struct {
	cfg  *Config
	lsns []func(event int, ctx interface{})
}

// AddListener attaches server event watcher.
func (b *Broker) AddListener(l func(event int, ctx interface{})) {
	b.lsns = append(b.lsns, l)
}

// Start configures local job broker.
func (b *Broker) Init(cfg *Config) (ok bool, err error) {
	b.cfg = cfg

	return true, nil
}
