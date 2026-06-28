package sterling

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClient_claim(t *testing.T) {
	c, err := New(t.Context(), WithMemoryClient(t.Name()))
	require.NoError(t, err)

	t.Cleanup(func() { c.Close() })

	t.Run("claiming a job with an empty queue", func(t *testing.T) {
		claim, err := c.claim(t.Context(), []string{"none"}, 0)
		assert.NoError(t, err)
		assert.Nil(t, claim)
	})

	t.Run("claiming a job with a pushed job", func(t *testing.T) {
		err := c.Push(t.Context(), "default", "test")
		require.NoError(t, err)

		claim, err := c.claim(t.Context(), []string{"default"}, 0)
		require.NoError(t, err)
		require.NotNil(t, claim)

		err = c.finish(t.Context(), claim.ID)
		assert.NoError(t, err)
	})

	t.Run("claiming a job and failing", func(t *testing.T) {
		err := c.Push(t.Context(), "default", "failure")
		require.NoError(t, err)

		claim, err := c.claim(t.Context(), []string{"default"}, 0)
		require.NoError(t, err)
		require.NotNil(t, claim)

		err = c.fail(t.Context(), claim.ID, time.Minute, "something went wrong")
		assert.NoError(t, err)
	})
}

func TestClient_ExtendLease(t *testing.T) {
	c, err := New(t.Context(), WithMemoryClient(t.Name()))
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })

	err = c.Push(t.Context(), "default", "test")
	require.NoError(t, err)

	claim, err := c.claim(t.Context(), []string{"default"}, 1)
	require.NoError(t, err)
	require.NotNil(t, claim)

	err = c.ExtendLease(t.Context(), claim.ID, 1)
	assert.NoError(t, err)
}

func TestExtendLease_contextErrors(t *testing.T) {
	t.Run("no client in context", func(t *testing.T) {
		err := ExtendLease(t.Context(), &Job{ID: 1})
		assert.ErrorContains(t, err, "no client found in context")
	})

	t.Run("no worker ID in context", func(t *testing.T) {
		c, err := New(t.Context(), WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })

		ctx := context.WithValue(t.Context(), contextClient{}, c)
		err = ExtendLease(ctx, &Job{ID: 1})
		assert.ErrorContains(t, err, "no worker ID found in context")
	})
}

func TestDeadJob(t *testing.T) {
	t.Run("fail with negative timeout marks job as dead", func(t *testing.T) {
		c, err := New(t.Context(), WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })

		require.NoError(t, c.Push(t.Context(), "default", "dead-job"))

		claim, err := c.claim(t.Context(), []string{"default"}, 0)
		require.NoError(t, err)
		require.NotNil(t, claim)

		require.NoError(t, c.fail(t.Context(), claim.ID, -1, "too many attempts"))

		var status string
		err = c.db.QueryRowContext(t.Context(), `SELECT "status" FROM "sterling_jobs" WHERE "id" = ?`, claim.ID).Scan(&status)
		require.NoError(t, err)
		assert.Equal(t, "dead", status)
	})

	t.Run("dead job records failure info", func(t *testing.T) {
		c, err := New(t.Context(), WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })

		require.NoError(t, c.Push(t.Context(), "default", "dead-job"))

		claim, err := c.claim(t.Context(), []string{"default"}, 0)
		require.NoError(t, err)
		require.NotNil(t, claim)

		require.NoError(t, c.fail(t.Context(), claim.ID, -1, "something went fatally wrong"))

		var info string
		err = c.db.QueryRowContext(t.Context(), `SELECT "failure_info" FROM "sterling_jobs" WHERE "id" = ?`, claim.ID).Scan(&info)
		require.NoError(t, err)
		assert.Equal(t, "something went fatally wrong", info)
	})

	t.Run("dead job is not claimed again", func(t *testing.T) {
		c, err := New(t.Context(), WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })

		require.NoError(t, c.Push(t.Context(), "default", "dead-job"))

		claim, err := c.claim(t.Context(), []string{"default"}, 0)
		require.NoError(t, err)
		require.NotNil(t, claim)

		require.NoError(t, c.fail(t.Context(), claim.ID, -1, "dead"))

		claim, err = c.claim(t.Context(), []string{"default"}, 0)
		require.NoError(t, err)
		assert.Nil(t, claim)
	})

	t.Run("dead job releases claim fields", func(t *testing.T) {
		c, err := New(t.Context(), WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })

		require.NoError(t, c.Push(t.Context(), "default", "dead-job"))

		claim, err := c.claim(t.Context(), []string{"default"}, 0)
		require.NoError(t, err)
		require.NotNil(t, claim)

		require.NoError(t, c.fail(t.Context(), claim.ID, -1, "dead"))

		var claimedAt, claimedTTL, claimedBy *int64
		err = c.db.QueryRowContext(t.Context(),
			`SELECT "claimed_at", "claimed_ttl", "claimed_by" FROM "sterling_jobs" WHERE "id" = ?`, claim.ID,
		).Scan(&claimedAt, &claimedTTL, &claimedBy)
		require.NoError(t, err)
		assert.Nil(t, claimedAt)
		assert.Nil(t, claimedTTL)
		assert.Nil(t, claimedBy)
	})
}

func TestClearDeadJobs(t *testing.T) {
	t.Run("safe on empty queue", func(t *testing.T) {
		c, err := New(t.Context(), WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })

		assert.NoError(t, c.ClearDeadJobs(t.Context()))
	})

	t.Run("removes dead jobs", func(t *testing.T) {
		c, err := New(t.Context(), WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })

		require.NoError(t, c.Push(t.Context(), "default", "dead-job"))

		claim, err := c.claim(t.Context(), []string{"default"}, 0)
		require.NoError(t, err)
		require.NotNil(t, claim)

		require.NoError(t, c.fail(t.Context(), claim.ID, -1, "dead"))

		var count int
		require.NoError(t, c.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM "sterling_jobs" WHERE "status" = 'dead'`).Scan(&count))
		assert.Equal(t, 1, count)

		require.NoError(t, c.ClearDeadJobs(t.Context()))

		require.NoError(t, c.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM "sterling_jobs" WHERE "status" = 'dead'`).Scan(&count))
		assert.Equal(t, 0, count)
	})

	t.Run("does not remove pending or finished jobs", func(t *testing.T) {
		c, err := New(t.Context(), WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })

		require.NoError(t, c.Push(t.Context(), "default", "pending-job"))
		require.NoError(t, c.Push(t.Context(), "default", "dead-job"))

		claim, err := c.claim(t.Context(), []string{"default"}, 0)
		require.NoError(t, err)
		require.NotNil(t, claim)

		require.NoError(t, c.fail(t.Context(), claim.ID, -1, "dead"))

		require.NoError(t, c.ClearDeadJobs(t.Context()))

		var total int
		require.NoError(t, c.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM "sterling_jobs"`).Scan(&total))
		assert.Equal(t, 1, total)

		var status string
		require.NoError(t, c.db.QueryRowContext(t.Context(), `SELECT "status" FROM "sterling_jobs"`).Scan(&status))
		assert.Equal(t, "pending", status)
	})

	t.Run("clears multiple dead jobs", func(t *testing.T) {
		c, err := New(t.Context(), WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })

		for range 3 {
			require.NoError(t, c.Push(t.Context(), "default", "dead-job"))
			claim, err := c.claim(t.Context(), []string{"default"}, 0)
			require.NoError(t, err)
			require.NotNil(t, claim)
			require.NoError(t, c.fail(t.Context(), claim.ID, -1, "dead"))
		}

		require.NoError(t, c.ClearDeadJobs(t.Context()))

		var count int
		require.NoError(t, c.db.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM "sterling_jobs" WHERE "status" = 'dead'`).Scan(&count))
		assert.Equal(t, 0, count)
	})
}

type testBackoffWorker struct {
	executeErr      error
	backoff         time.Duration
	capturedQueue   string
	capturedKind    string
	capturedAttempt int64
}

func (w *testBackoffWorker) Execute(_ context.Context, _ *Job) error { return w.executeErr }

func (w *testBackoffWorker) BackoffForAttemp(queue, kind string, attempt int64) time.Duration {
	w.capturedQueue = queue
	w.capturedKind = kind
	w.capturedAttempt = attempt
	return w.backoff
}

func TestWorkerBackoff(t *testing.T) {
	t.Run("BackoffForAttemp is called with correct args", func(t *testing.T) {
		c, err := New(t.Context(), WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })

		worker := &testBackoffWorker{executeErr: errTest, backoff: time.Minute}
		c.Register("backoff-job", worker)

		require.NoError(t, c.Push(t.Context(), "my-queue", "backoff-job"))
		_ = c.Step(t.Context(), []string{"my-queue"})

		assert.Equal(t, "my-queue", worker.capturedQueue)
		assert.Equal(t, "backoff-job", worker.capturedKind)
		assert.Equal(t, int64(1), worker.capturedAttempt)
	})

	t.Run("negative backoff marks job dead", func(t *testing.T) {
		c, err := New(t.Context(), WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })

		worker := &testBackoffWorker{executeErr: errTest, backoff: -1}
		c.Register("backoff-job", worker)

		require.NoError(t, c.Push(t.Context(), "default", "backoff-job"))
		_ = c.Step(t.Context(), []string{"default"})

		var status string
		require.NoError(t, c.db.QueryRowContext(t.Context(), `SELECT "status" FROM "sterling_jobs"`).Scan(&status))
		assert.Equal(t, "dead", status)
	})

	t.Run("zero backoff marks job finished", func(t *testing.T) {
		c, err := New(t.Context(), WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })

		worker := &testBackoffWorker{executeErr: errTest, backoff: 0}
		c.Register("backoff-job", worker)

		require.NoError(t, c.Push(t.Context(), "default", "backoff-job"))
		_ = c.Step(t.Context(), []string{"default"})

		var status string
		require.NoError(t, c.db.QueryRowContext(t.Context(), `SELECT "status" FROM "sterling_jobs"`).Scan(&status))
		assert.Equal(t, "finished", status)
	})

	t.Run("positive backoff re-queues with future visible_at", func(t *testing.T) {
		c, err := New(t.Context(), WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })

		worker := &testBackoffWorker{executeErr: errTest, backoff: 5 * time.Minute}
		c.Register("backoff-job", worker)

		require.NoError(t, c.Push(t.Context(), "default", "backoff-job"))
		_ = c.Step(t.Context(), []string{"default"})

		var status string
		var visibleAt int64
		require.NoError(t, c.db.QueryRowContext(t.Context(), `SELECT "status", "visible_at" FROM "sterling_jobs"`).Scan(&status, &visibleAt))
		assert.Equal(t, "pending", status)
		assert.Greater(t, visibleAt, time.Now().Unix())
	})

	t.Run("positive backoff job is not immediately re-claimable", func(t *testing.T) {
		c, err := New(t.Context(), WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })

		worker := &testBackoffWorker{executeErr: errTest, backoff: time.Hour}
		c.Register("backoff-job", worker)

		require.NoError(t, c.Push(t.Context(), "default", "backoff-job"))
		_ = c.Step(t.Context(), []string{"default"})

		claim, err := c.claim(t.Context(), []string{"default"}, 0)
		require.NoError(t, err)
		assert.Nil(t, claim)
	})

	t.Run("falls back to default backoff when interface not implemented", func(t *testing.T) {
		c, err := New(t.Context(), WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { c.Close() })

		c.jobBackoff = func(_ Worker, _, _ string, _ int64) time.Duration { return -1 }
		c.RegisterFunc("plain-job", func(_ context.Context, _ *Job) error { return errTest })

		require.NoError(t, c.Push(t.Context(), "default", "plain-job"))
		_ = c.Step(t.Context(), []string{"default"})

		var status string
		require.NoError(t, c.db.QueryRowContext(t.Context(), `SELECT "status" FROM "sterling_jobs"`).Scan(&status))
		assert.Equal(t, "dead", status)
	})
}

var errTest = errors.New("intentional test failure")

func BenchmarkStep(b *testing.B) {
	client, err := New(b.Context(), WithMemoryClient(b.Name()))
	require.NoError(b, err)
	b.Cleanup(func() { client.Close() })

	client.RegisterFunc("test", func(context.Context, *Job) error {
		return nil
	})

	for range b.N {
		err := client.Push(b.Context(), "default", "test")
		require.NoError(b, err)
	}

	for b.Loop() {
		client.Step(b.Context(), []string{"default"})
	}
}
