# go-sterling

A SQLite-backed job queue for Go. No external dependencies beyond a SQLite driver — sterling stores jobs in a local database and processes them with an in-process worker pool.

## Installation

```sh
go get github.com/maddiesch/go-sterling
```

## Quick Start

```go
client, err := sterling.New(ctx, sterling.WithDatabaseFile("jobs.db"))
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Register a worker for the "send-email" job kind.
client.Register("send-email", sterling.ValueWorker(func(ctx context.Context, job *sterling.Job, to string) error {
    return sendEmail(to)
}))

// Enqueue a job.
if err := client.Push(ctx, "default", "send-email", sterling.PushJSON("user@example.com")); err != nil {
    log.Fatal(err)
}

// Block and process jobs until ctx is cancelled.
if err := client.Run(ctx, []string{"default"}, 4); err != nil {
    log.Fatal(err)
}
```

## Concepts

### Queues

A queue is a named channel that groups related jobs. Queues are created automatically the first time a job is pushed to them. A single `Run` call can drain multiple queues concurrently.

### Jobs

Each job has a **kind** (which worker handles it) and an optional **payload** (arbitrary bytes, usually JSON). Jobs move through the states `pending → claimed → finished` (or back to `pending` on failure).

### Workers

A `Worker` is anything that implements `Execute(context.Context, *Job) error`. Returning a non-nil error marks the job as failed and schedules a retry with exponential-ish backoff. Three helpers cover the common cases:

| Helper                   | Use when                                                 |
| ------------------------ | -------------------------------------------------------- |
| `Register(kind, worker)` | You have a `Worker` implementation                       |
| `RegisterFunc(kind, fn)` | You have a plain `func(context.Context, *Job) error`     |
| `ValueWorker[T](fn)`     | Your payload is JSON you want auto-unmarshalled into `T` |

### Retries & Backoff

Jobs are retried up to 10 times (default). The default backoff is `attempt * 1 minute`. On each retry `job.Attempt` increments so workers can branch on it.

## Configuration

```go
// In-memory (good for tests)
sterling.New(ctx, sterling.WithMemoryClient("test-instance"))

// File-backed
sterling.New(ctx, sterling.WithDatabaseFile("/var/lib/app/jobs.db"))

// Bring your own *sql.DB (caller owns lifecycle)
sterling.New(ctx, sterling.WithDatabase(myDB))
```

## Push Options

```go
client.Push(ctx, "default", "resize-image",
    sterling.PushJSON(payload),           // JSON payload
    func(p *sterling.Push) error {        // schedule for future visibility
        p.VisibleAt = time.Now().Add(5 * time.Minute)
        return nil
    },
    func(p *sterling.Push) error {        // expire if not processed in time
        p.ExpiresAt = time.Now().Add(24 * time.Hour)
        return nil
    },
)
```

## Long-Running Jobs

Workers have a 300-second claim TTL. Call `sterling.ExtendLease` inside the worker to reset it:

```go
client.Register("transcode", sterling.WorkerFunc(func(ctx context.Context, job *sterling.Job) error {
    for chunk := range chunks {
        if err := sterling.ExtendLease(ctx, job); err != nil {
            return err
        }
        process(chunk)
    }
    return nil
}))
```

## Cleanup

Finished jobs are kept for one hour. Expired and stale claimed jobs are returned to the queue. Run cleanup on a timer:

```go
ticker := time.NewTicker(30 * time.Second)
for range ticker.C {
    client.Cleanup(ctx)
}
```

## Stats

```go
queues, _ := client.ListQueue(ctx)
for _, name := range queues {
    stat, _ := client.LoadQueueStat(ctx, name)
    fmt.Printf("%s: %+v\n", name, stat)
}
```

## See Also

The `example/` directory contains a runnable demo with a poller, multiple workers, delayed jobs, retry simulation, and a final stats dump.
