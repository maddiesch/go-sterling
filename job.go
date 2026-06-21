package sterling

import (
	"context"
	"fmt"
)

type Job struct {
	ID      int64
	Kind    string
	Payload []byte
	Attempt int64
}

func ExtendLease(ctx context.Context, job *Job) error {
	client, _ := ctx.Value(contextClient{}).(*Client)
	if client == nil {
		return fmt.Errorf("no client found in context")
	}
	workerID, _ := ctx.Value(contextWorkerID{}).(int64)
	if workerID == 0 {
		return fmt.Errorf("no worker ID found in context")
	}

	return client.ExtendLease(ctx, job.ID, workerID)
}
