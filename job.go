package sterling

import (
	"context"
	"fmt"
	"time"
)

// Job represents a job being processed by a Worker. The Payload field contains
// the raw bytes pushed via PushOption (typically JSON). Attempt is 1-indexed
// and increments on each retry.
type Job struct {
	ID        int64
	Kind      string
	Payload   []byte
	Attempt   int64
	CreatedAt time.Time
}

// ExtendLease extends the claim TTL for the given job. It reads the client and
// worker ID from ctx, which are injected automatically when a worker is running
// inside Client.Run. Call this from long-running workers to avoid having the
// job reclaimed by Cleanup.
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
