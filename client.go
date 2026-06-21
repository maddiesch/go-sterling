package sterling

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	defaultClaimTTL = 300
)

type Client struct {
	db            *sql.DB
	shutdown      func() error
	workerMu      sync.RWMutex
	workers       map[string]Worker
	workerID      atomic.Int64
	pollerID      atomic.Int64
	pollerBackoff time.Duration
	maxAttempts   int
	jobBackoff    func(string, string, int64) time.Duration
}

type Option func(context.Context, *Client) error

func WithMemoryClient(name string) Option {
	if name == "" {
		random := make([]byte, 8)
		if _, err := rand.Read(random); err != nil {
			return func(context.Context, *Client) error {
				return fmt.Errorf("failed to generate random bytes: %w", err)
			}
		}
		name = hex.EncodeToString(random)
	}

	name = base64.RawURLEncoding.EncodeToString([]byte(name))

	dbPath := fmt.Sprintf("file:%s?mode=memory&cache=shared&_busy_timeout=5000", name)

	return WithDatabaseURL(dbPath)
}

// WithDatabase allows you to provide your own database connection.
// *Note:* The caller owns the lifecycle of the database connection and is responsible for closing it when it's no longer needed.
func WithDatabase(db *sql.DB) Option {
	return func(_ context.Context, client *Client) error {
		if client.db != nil {
			return errors.New("database already configured for client")
		}

		client.db = db

		return nil
	}
}

func WithDatabaseFile(path string) Option {
	return WithDatabaseURL(fmt.Sprintf("file:%s?mode=rwc&cache=shared&_busy_timeout=5000", path))
}

func WithDatabaseURL(url string) Option {
	return func(ctx context.Context, client *Client) error {
		if client.db != nil {
			return errors.New("database already configured for client")
		}

		db, err := sql.Open("sqlite3", url)
		if err != nil {
			return fmt.Errorf("failed to open database: %w", err)
		}

		db.SetConnMaxIdleTime(2 * time.Minute)
		db.SetConnMaxLifetime(5 * time.Minute)
		db.SetMaxIdleConns(1)
		db.SetMaxOpenConns(1)

		if err := db.PingContext(ctx); err != nil {
			return fmt.Errorf("failed to ping database: %w", err)
		}

		if _, err := db.ExecContext(ctx, `PRAGMA journal_mode = WAL;`); err != nil {
			_ = db.Close()
			return fmt.Errorf("failed to set journal mode: %w", err)
		}

		client.db = db
		client.shutdown = db.Close

		return nil
	}
}

func New(ctx context.Context, options ...Option) (*Client, error) {
	if len(options) == 0 {
		options = append(options, WithMemoryClient(""))
	}

	client := new(Client)
	client.workers = make(map[string]Worker)
	client.pollerBackoff = 1 * time.Second
	client.maxAttempts = 10
	client.jobBackoff = func(_, _ string, attempt int64) time.Duration {
		return time.Duration(attempt) * time.Minute
	}

	for _, option := range options {
		if err := option(ctx, client); err != nil {
			return nil, err
		}
	}

	if client.db == nil {
		return nil, errors.New("no database configured for client")
	}

	if err := client.prepare(ctx); err != nil {
		return nil, fmt.Errorf("failed to prepare client: %w", err)
	}

	return client, nil
}

// Register a worker to handle jobs of a specific kind
func (c *Client) Register(kind string, worker Worker) {
	c.workerMu.Lock()
	c.workers[kind] = worker
	c.workerMu.Unlock()
}

func (c *Client) RegisterFunc(kind string, worker WorkerFunc) {
	c.Register(kind, worker)
}

func (c *Client) prepare(ctx context.Context) error {
	slog.DebugContext(ctx, "Setup Sterling DB")

	if _, err := c.db.ExecContext(ctx, createJobTableSQL); err != nil {
		return fmt.Errorf("failed to create jobs table: %w", err)
	}
	if _, err := c.db.ExecContext(ctx, createJobStatsTableSQL); err != nil {
		return fmt.Errorf("failed to create job stats table: %w", err)
	}
	if _, err := c.db.ExecContext(ctx, createQueueTableSQL); err != nil {
		return fmt.Errorf("failed to create queues table: %w", err)
	}

	return nil
}

type Push struct {
	Payload   []byte
	Priority  int
	VisibleAt time.Time
	ExpiresAt time.Time
}

type PushOption func(*Push) error

func PushJSON(value any) PushOption {
	return func(p *Push) error {
		data, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("failed to marshal JSON: %w", err)
		}
		p.Payload = data
		return nil
	}
}

func (c *Client) Push(ctx context.Context, queue, kind string, options ...PushOption) error {
	var push Push
	for _, option := range options {
		if err := option(&push); err != nil {
			return fmt.Errorf("failed to apply push option: %w", err)
		}
	}
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	const insertQueueSQL = `
	INSERT OR IGNORE INTO "sterling_queues" ("name") VALUES (?);
	`

	columns := []string{`"status"`, `"queue"`, `"kind"`, `"payload"`, `"priority"`}
	arguments := []string{`'pending'`, `?`, `?`, `?`, `?`}
	values := []any{
		queue,
		kind,
		push.Payload,
		push.Priority,
	}
	if !push.VisibleAt.IsZero() {
		columns = append(columns, `"visible_at"`)
		arguments = append(arguments, `?`)
		values = append(values, push.VisibleAt.Unix())
	}
	if !push.ExpiresAt.IsZero() {
		columns = append(columns, `"expires_at"`)
		arguments = append(arguments, `?`)
		values = append(values, push.ExpiresAt.Unix())
	}

	var insertJobSQL = fmt.Sprintf(`INSERT INTO "sterling_jobs" (%s) VALUES (%s);`, strings.Join(columns, ", "), strings.Join(arguments, ", "))

	const insertStatsSQL = `
	INSERT INTO "sterling_job_stats" ("queue", "kind", "total_jobs") VALUES (?, ?, 1)
	ON CONFLICT ("queue", "kind") DO UPDATE SET "total_jobs" = "total_jobs" + 1;
	`

	if _, err := tx.ExecContext(ctx, insertQueueSQL, queue); err != nil {
		return fmt.Errorf("failed to insert queue: %w", err)
	}

	if _, err := tx.ExecContext(ctx, insertJobSQL, values...); err != nil {
		return fmt.Errorf("failed to insert job: %w", err)
	}

	if _, err := tx.ExecContext(ctx, insertStatsSQL, queue, kind); err != nil {
		return fmt.Errorf("failed to insert job stats: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	slog.DebugContext(ctx, "Push", slog.String("queue", queue), slog.String("kind", kind))

	return nil
}

func (c *Client) finish(ctx context.Context, jobID int64) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	const updateJobSQL = `
	UPDATE "sterling_jobs"
	SET "status" = 'finished',
			"finished_at" = unixepoch()
	WHERE id = ?;
	`

	_, err = tx.ExecContext(ctx, updateJobSQL, jobID)
	if err != nil {
		return fmt.Errorf("failed to update job: %w", err)
	}

	const updateStatsSQL = `
	WITH finished AS (
		SELECT "queue", "kind" FROM "sterling_jobs" WHERE id = ? LIMIT 1
	)
	UPDATE "sterling_job_stats"
	SET "completed_jobs" = "completed_jobs" + 1,
		"last_completed_at" = unixepoch()
	WHERE ("queue", "kind") IN (SELECT "queue", "kind" FROM finished);
	`

	_, err = tx.ExecContext(ctx, updateStatsSQL, jobID)
	if err != nil {
		return fmt.Errorf("failed to update job stats: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}
	return nil
}

func (c *Client) fail(ctx context.Context, jobID int64, timeout time.Duration, info string) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	const updateJobSQL = `
	UPDATE "sterling_jobs"
	SET "status" = 'pending',
			"visible_at" = unixepoch() + ?,
			"failure_info" = ?,
			"claimed_at" = NULL,
			"claimed_ttl" = NULL,
			"claimed_by" = NULL
	WHERE id = ?;
	`

	_, err = tx.ExecContext(ctx, updateJobSQL, int(timeout.Seconds()), info, jobID)
	if err != nil {
		return fmt.Errorf("failed to update job: %w", err)
	}

	const updateStatsSQL = `
	WITH finished AS (
		SELECT "queue", "kind" FROM "sterling_jobs" WHERE id = ? LIMIT 1
	)
	UPDATE "sterling_job_stats"
	SET "failed_jobs" = "failed_jobs" + 1,
		"last_failed_at" = unixepoch()
	WHERE ("queue", "kind") IN (SELECT "queue", "kind" FROM finished);
	`
	_, err = tx.ExecContext(ctx, updateStatsSQL, jobID)
	if err != nil {
		return fmt.Errorf("failed to update job stats: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

type jobClaim struct {
	ID      int64
	Queue   string
	Kind    string
	Payload []byte
	Attempt int64
}

func (c *Client) claim(ctx context.Context, queues []string, workerID int64) (*jobClaim, error) {
	tx, err := c.db.Conn(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get database connection: %w", err)
	}
	defer tx.Close()

	var committed bool
	if _, err := tx.ExecContext(ctx, `BEGIN EXCLUSIVE TRANSACTION;`); err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if !committed {
			if _, err := tx.ExecContext(context.WithoutCancel(ctx), `ROLLBACK;`); err != nil {
				slog.ErrorContext(ctx, "Failed to rollback transaction", slog.String("error", err.Error()))
			}
		}
	}()

	placeholders := strings.Repeat("?,", len(queues))
	placeholders = placeholders[:len(placeholders)-1]

	claimSQL := fmt.Sprintf(`
	WITH candidates AS (
		SELECT id FROM "sterling_jobs"
		WHERE "queue" IN (%s)
			AND "status" = 'pending'
			AND ("expires_at" IS NULL OR "expires_at" > unixepoch())
			AND "visible_at" <= unixepoch()
			AND "current_attempt" < ?
		ORDER BY "priority" DESC, "created_at" ASC
		LIMIT 1
	)
	UPDATE "sterling_jobs"
		SET "status" = 'claimed',
				"claimed_at" = unixepoch(),
				"claimed_by" = ?,
				"claimed_ttl" = ?,
				"current_attempt" = "current_attempt" + 1
	WHERE "id" = (SELECT "id" FROM candidates)
	RETURNING "id", "queue", "kind", "payload", "current_attempt";
	`, placeholders)

	args := make([]any, 0, len(queues)+2)
	for _, q := range queues {
		args = append(args, q)
	}
	args = append(args, c.maxAttempts, workerID, defaultClaimTTL)

	row := tx.QueryRowContext(ctx, claimSQL, args...)
	var (
		jobID   int64
		queue   string
		kind    string
		payload []byte
		attempt int64
	)
	if err := row.Scan(&jobID, &queue, &kind, &payload, &attempt); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to claim job: %w", err)
	}

	if _, err := tx.ExecContext(ctx, `COMMIT;`); err != nil {
		return nil, fmt.Errorf("failed to commit transaction: %w", err)
	}

	committed = true

	return &jobClaim{
		ID:      jobID,
		Queue:   queue,
		Kind:    kind,
		Payload: payload,
		Attempt: attempt,
	}, nil
}

func (c *Client) Run(ctx context.Context, queues []string, workers int) error {
	var wait sync.WaitGroup
	wait.Add(workers)

	jobChan := make(chan *jobClaim, workers)
	pollerID := c.pollerID.Add(1)

	pCtx, cancel := context.WithCancelCause(ctx)
	pCtx = context.WithValue(pCtx, contextPollerID{}, pollerID)
	pCtx = context.WithValue(pCtx, contextClient{}, c)

	go func() {
		defer close(jobChan)
		defer cancel(nil)

		slog.DebugContext(ctx, "Starting Poller", slog.Int64("poller-id", pollerID), slog.String("queues", strings.Join(queues, ",")))
		defer slog.DebugContext(ctx, "Stopping Poller", slog.Int64("poller-id", pollerID))

		for {
			select {
			case <-ctx.Done():
				return
			default:
				claim, err := c.claim(ctx, queues, pollerID)
				if err != nil {
					cancel(err)
					return
				}
				if claim == nil {
					timer := time.NewTimer(c.pollerBackoff)
					select {
					case <-ctx.Done():
						timer.Stop()
						return
					case <-timer.C:
						continue
					}
				}

				select {
				case <-ctx.Done():
					return
				case jobChan <- claim:
				}
			}
		}
	}()

	for range workers {
		go func() {
			defer wait.Done()

			workerID := c.workerID.Add(1)
			wCtx := context.WithValue(pCtx, contextWorkerID{}, workerID)

			slog.DebugContext(ctx, "Starting Worker", slog.Int64("poller-id", pollerID), slog.Int64("worker-id", workerID))
			defer slog.DebugContext(ctx, "Stopping Worker", slog.Int64("poller-id", pollerID), slog.Int64("worker-id", workerID))

			for {
				select {
				case <-ctx.Done():
					return
				case job, ok := <-jobChan:
					if !ok {
						return
					}

					c.process(wCtx, pollerID, workerID, job)
				}
			}
		}()
	}

	wait.Wait()

	if cause := context.Cause(pCtx); cause != nil && !errors.Is(cause, context.Canceled) {
		return cause
	}

	return nil
}

func (c *Client) Close() error {
	if c.shutdown != nil {
		return c.shutdown()
	}

	return nil
}

func (c *Client) Cleanup(ctx context.Context) error {
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback()

	if err := c.sweepFinishedJobs(ctx, tx); err != nil {
		return err
	}

	if err := c.sweepExpiredJobs(ctx, tx); err != nil {
		return err
	}

	if err := c.sweepExpiredClaims(ctx, tx); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

func (c *Client) sweepFinishedJobs(ctx context.Context, tx *sql.Tx) error {
	const sweepSQL = `
	DELETE FROM "sterling_jobs"
	WHERE "status" = 'finished'
	AND "finished_at" + 3600 <= unixepoch();
	`

	result, err := tx.ExecContext(ctx, sweepSQL)
	if err != nil {
		return fmt.Errorf("failed to sweep jobs: %w", err)
	}

	count, _ := result.RowsAffected()

	slog.DebugContext(ctx, "Cleanup Finished Jobs", slog.Int64("removed", count))

	return nil
}

func (c *Client) sweepExpiredJobs(ctx context.Context, tx *sql.Tx) error {
	const sweepSQL = `
	DELETE FROM "sterling_jobs"
	WHERE "status" = 'pending'
	AND "expires_at" IS NOT NULL
	AND "expires_at" <= unixepoch();
	`

	result, err := tx.ExecContext(ctx, sweepSQL)
	if err != nil {
		return fmt.Errorf("failed to sweep jobs: %w", err)
	}

	count, _ := result.RowsAffected()

	slog.DebugContext(ctx, "Cleanup Expired Jobs", slog.Int64("removed", count))

	return nil
}

func (c *Client) sweepExpiredClaims(ctx context.Context, tx *sql.Tx) error {
	const sweepSQL = `
	UPDATE "sterling_jobs"
	SET "status" = 'pending',
			"claimed_at" = NULL,
			"claimed_ttl" = NULL,
			"claimed_by" = NULL,
			"claim_timeout" = "claim_timeout" + 1
	WHERE "status" = 'claimed'
	AND "claimed_at" IS NOT NULL
	AND "claimed_at" + "claimed_ttl" <= unixepoch();
	`

	result, err := tx.ExecContext(ctx, sweepSQL)
	if err != nil {
		return fmt.Errorf("failed to sweep jobs: %w", err)
	}

	count, _ := result.RowsAffected()

	slog.DebugContext(ctx, "Cleanup Expired Claims", slog.Int64("removed", count))

	return nil
}

func (c *Client) process(ctx context.Context, pollerID, workerID int64, claim *jobClaim) {
	c.workerMu.RLock()
	worker, ok := c.workers[claim.Kind]
	c.workerMu.RUnlock()

	if !ok {
		slog.ErrorContext(ctx, "No worker registered for job kind", slog.String("kind", claim.Kind))
		if err := c.fail(ctx, claim.ID, time.Minute, "no worker registered for job kind"); err != nil {
			slog.ErrorContext(ctx, "Failed to mark job as failed", slog.Int64("job_id", claim.ID), slog.String("error", err.Error()))
		}
		return
	}

	job := &Job{
		ID:      claim.ID,
		Kind:    claim.Kind,
		Payload: claim.Payload,
		Attempt: claim.Attempt,
	}
	err := worker.Execute(ctx, job)

	if err != nil {
		slog.ErrorContext(ctx, "Worker failed to execute job", slog.Int64("job-id", claim.ID), slog.String("error", err.Error()))

		timeout := c.jobBackoff(claim.Queue, claim.Kind, claim.Attempt)
		if err := c.fail(ctx, claim.ID, timeout, err.Error()); err != nil {
			slog.ErrorContext(ctx, "Failed to mark job as failed", slog.Duration("timeout", timeout), slog.Int64("job-id", claim.ID), slog.String("error", err.Error()))
		}
	} else {
		if err := c.finish(ctx, claim.ID); err != nil {
			slog.ErrorContext(ctx, "Failed to mark job as finished", slog.Int64("job-id", claim.ID), slog.String("error", err.Error()))
		}
	}
}

type contextWorkerID struct{}
type contextPollerID struct{}
type contextClient struct{}

const createJobTableSQL = `
CREATE TABLE IF NOT EXISTS "sterling_jobs" (
	"id" INTEGER PRIMARY KEY AUTOINCREMENT,
  "status" TEXT NOT NULL,
	"queue" TEXT NOT NULL,
	"kind" TEXT NOT NULL,
	"payload" BLOB,
	"priority" INTEGER NOT NULL DEFAULT 0,
	"created_at" INTEGER NOT NULL DEFAULT (unixepoch()),
	"visible_at" INTEGER NOT NULL DEFAULT (unixepoch()),
	"current_attempt" INTEGER NOT NULL DEFAULT 0,
	"failure_info" TEXT,
	"expires_at" INTEGER,
	"claimed_at" INTEGER,
	"claimed_ttl" INTEGER,
	"claimed_by" INTEGER,
	"claim_timeout" INTEGER NOT NULL DEFAULT 0,
	"finished_at" INTEGER
);

CREATE INDEX IF NOT EXISTS "idx_sterling_jobs_queue_status" ON "sterling_jobs" ("queue", "status", "expires_at", "visible_at", "priority");
CREATE INDEX IF NOT EXISTS "idx_sterling_jobs_status" ON "sterling_jobs" ("status", "claimed_at");
CREATE INDEX IF NOT EXISTS "idx_sterling_jobs_status" ON "sterling_jobs" ("status", "finished_at");
CREATE INDEX IF NOT EXISTS "idx_sterling_jobs_expires" ON "sterling_jobs" ("expires_at");
`

const createJobStatsTableSQL = `
CREATE TABLE IF NOT EXISTS "sterling_job_stats" (
	"queue" TEXT NOT NULL,
	"kind" TEXT NOT NULL,
	"total_jobs" INTEGER NOT NULL DEFAULT 0,
	"completed_jobs" INTEGER NOT NULL DEFAULT 0,
	"failed_jobs" INTEGER NOT NULL DEFAULT 0,
	"last_completed_at" INTEGER,
	"last_failed_at" INTEGER
);

CREATE UNIQUE INDEX IF NOT EXISTS "idx_sterling_job_stats_queue_kind" ON "sterling_job_stats" ("queue", "kind");
`

const createQueueTableSQL = `
CREATE TABLE IF NOT EXISTS "sterling_queues" (
	"name" TEXT PRIMARY KEY,
	"created_at" INTEGER NOT NULL DEFAULT (unixepoch())
);
`

type WorkerFunc func(context.Context, *Job) error

func (f WorkerFunc) Execute(ctx context.Context, job *Job) error {
	return f(ctx, job)
}

func ValueWorker[T any](handler func(context.Context, *Job, T) error) Worker {
	return WorkerFunc(func(ctx context.Context, job *Job) error {
		var value T
		if err := json.Unmarshal(job.Payload, &value); err != nil {
			return fmt.Errorf("failed to unmarshal job payload: %w", err)
		}
		return handler(ctx, job, value)
	})
}

var (
	_ Worker = WorkerFunc(nil)
)
