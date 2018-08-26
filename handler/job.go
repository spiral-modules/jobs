package handler

type Job struct {
	Job      string `json:"job"`
	Pipeline string `json:"pipeline"`
	Payload  interface{}
	Options struct {
		Delay *int `json:"delay"`
	}
}
