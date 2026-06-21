package sterling

type Job struct {
	ID      int64
	Kind    string
	Payload []byte
	Attempt int64
}
