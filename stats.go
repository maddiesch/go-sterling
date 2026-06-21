package sterling

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// QueueStat contains aggregate information about a queue and its job kinds.
type QueueStat struct {
	Name      string
	CreatedAt time.Time           `json:",omitzero"`
	Jobs      map[string]*JobStat `json:",omitempty"`
}

// JobStat holds lifetime counters for a single (queue, kind) pair.
type JobStat struct {
	Total          int64
	Finished       int64
	Failed         int64
	LastFinishedAt time.Time `json:",omitzero"`
	LastFailedAt   time.Time `json:",omitzero"`
}

// ListQueue returns the names of all queues that have ever had a job pushed to them.
func (c *Client) ListQueue(ctx context.Context) ([]string, error) {
	slog.DebugContext(ctx, "listing queues")

	rows, err := c.db.QueryContext(ctx, `SELECT "name" FROM "sterling_queues";`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var queues []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		queues = append(queues, name)
	}

	return queues, nil
}

// LoadQueueStat returns statistics for a queue broken down by job kind.
func (c *Client) LoadQueueStat(ctx context.Context, queue string) (*QueueStat, error) {
	var queueCreatedAt int64

	slog.DebugContext(ctx, "loading queue stats", slog.String("queue", queue))

	row := c.db.QueryRowContext(ctx, `SELECT "created_at" FROM "sterling_queues" WHERE "name" = ?`, queue)
	if err := row.Scan(&queueCreatedAt); err != nil {
		return nil, err
	}

	columns := []string{"kind", "total_jobs", "completed_jobs", "failed_jobs", "last_completed_at", "last_failed_at"}

	jobsQuery := fmt.Sprintf(`SELECT %s FROM "sterling_job_stats" WHERE "queue" = ?;`, strings.Join(columns, ", "))
	rows, err := c.db.QueryContext(ctx, jobsQuery, queue)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	jobs := make(map[string]*JobStat)

	for rows.Next() {
		var (
			kind                                 string
			totalJobs, completedJobs, failedJobs int64
			lastCompletedAt, lastFailedAt        *int64
		)
		if err := rows.Scan(&kind, &totalJobs, &completedJobs, &failedJobs, &lastCompletedAt, &lastFailedAt); err != nil {
			return nil, err
		}

		jobs[kind] = &JobStat{
			Total:    totalJobs,
			Finished: completedJobs,
			Failed:   failedJobs,
		}
		if lastCompletedAt != nil {
			jobs[kind].LastFinishedAt = time.Unix(*lastCompletedAt, 0)
		}
		if lastFailedAt != nil {
			jobs[kind].LastFailedAt = time.Unix(*lastFailedAt, 0)
		}
	}

	return &QueueStat{
		Name:      queue,
		CreatedAt: time.Unix(queueCreatedAt, 0),
		Jobs:      jobs,
	}, nil
}
