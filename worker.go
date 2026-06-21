package sterling

import (
	"context"
)

type Worker interface {
	Execute(context.Context, *Job) error
}
