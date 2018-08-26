package handler

type Handler interface {
	Handle(j *Job) (string, error)
	Serve() error
	Stop()
}
