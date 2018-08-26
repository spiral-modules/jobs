package handler

type Job struct {
	Job      string `json:"job"`
	Pipeline string `json:"pipeline"`
	Payload  string
	Options struct {
		Delay *int `json:"delay"`
	}
}
