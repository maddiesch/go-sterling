package sterling

import "context"

func ClientFromContext(ctx context.Context) *Client {
	c, _ := ctx.Value(contextClient{}).(*Client)
	return c
}

func PollerIDFromContext(ctx context.Context) int64 {
	pollerID, _ := ctx.Value(contextPollerID{}).(int64)
	return pollerID
}

func WorkerIDFromContext(ctx context.Context) int64 {
	workerID, _ := ctx.Value(contextWorkerID{}).(int64)
	return workerID
}
