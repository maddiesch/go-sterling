package sterling

import (
	"context"
	"time"
)

// Worker processes jobs claimed from a queue.
type Worker interface {
	Execute(context.Context, *Job) error
}

type WorkerBackoff interface {
	BackoffForAttemp(string, string, int64) time.Duration
}
