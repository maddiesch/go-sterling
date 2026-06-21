package sterling

import (
	"context"
)

// Worker processes jobs claimed from a queue.
type Worker interface {
	Execute(context.Context, *Job) error
}
