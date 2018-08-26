package jobs

type Handler interface {
	Handle() error
	Serve() error
	Stop()
}
