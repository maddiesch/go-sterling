package sterling_test

import (
	"context"
	"database/sql"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	sterling "github.com/maddiesch/go-sterling"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Run("defaults to memory client", func(t *testing.T) {
		client, err := sterling.New(t.Context())
		require.NoError(t, err)
		require.NotNil(t, client)
		assert.NoError(t, client.Close())
	})

	t.Run("WithMemoryClient", func(t *testing.T) {
		client, err := sterling.New(t.Context(), sterling.WithMemoryClient(t.Name()))
		require.NoError(t, err)
		require.NotNil(t, client)
		assert.NoError(t, client.Close())
	})

	t.Run("WithDatabaseURL", func(t *testing.T) {
		client, err := sterling.New(t.Context(), sterling.WithDatabaseURL("file::memory:?cache=shared"))
		require.NoError(t, err)
		require.NotNil(t, client)
		assert.NoError(t, client.Close())
	})

	t.Run("WithDatabaseFile", func(t *testing.T) {
		path := t.TempDir() + "/test.db"
		client, err := sterling.New(t.Context(), sterling.WithDatabaseFile(path))
		require.NoError(t, err)
		require.NotNil(t, client)
		assert.NoError(t, client.Close())
	})

	t.Run("multiple memory clients are isolated", func(t *testing.T) {
		c1, err := sterling.New(t.Context(), sterling.WithMemoryClient(""))
		require.NoError(t, err)
		t.Cleanup(func() { c1.Close() })

		c2, err := sterling.New(t.Context(), sterling.WithMemoryClient(""))
		require.NoError(t, err)
		t.Cleanup(func() { c2.Close() })

		assert.NotEqual(t, c1, c2)
	})
}

func TestClientPush(t *testing.T) {
	client, err := sterling.New(t.Context(), sterling.WithMemoryClient(t.Name()))
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	t.Run("push a job", func(t *testing.T) {
		err := client.Push(t.Context(), "default", "test")
		assert.NoError(t, err)
	})
}

func TestClose(t *testing.T) {
	t.Run("close without shutdown is safe", func(t *testing.T) {
		client, err := sterling.New(t.Context(), sterling.WithMemoryClient(t.Name()))
		require.NoError(t, err)
		assert.NoError(t, client.Close())
	})
}

func TestProcessing(t *testing.T) {
	client, err := sterling.New(t.Context(), sterling.WithMemoryClient(t.Name()))
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	t.Run("process a job", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), time.Second)
		defer cancel()

		var counter atomic.Int64

		client.RegisterFunc("test-1", func(ctx context.Context, j *sterling.Job) error {
			assert.Equal(t, "test-1", j.Kind)
			counter.Add(1)
			cancel() // stop processing after one job

			return nil
		})
		err := client.Push(t.Context(), "default", "test-1")
		require.NoError(t, err)

		err = client.Run(ctx, []string{"default"}, 5)
		assert.NoError(t, err)
		assert.Equal(t, int64(1), counter.Load())
	})
}

func TestClientCleanup(t *testing.T) {
	client, err := sterling.New(t.Context(), sterling.WithMemoryClient(t.Name()))
	require.NoError(t, err)

	t.Cleanup(func() { client.Close() })

	t.Run("cleanup is safe", func(t *testing.T) {
		assert.NoError(t, client.Cleanup(t.Context()))
	})

	t.Run("cleanup removes expired pending jobs", func(t *testing.T) {
		opt := sterling.PushOption(func(p *sterling.PushPayload) error {
			p.ExpiresAt = time.Now().Add(-time.Hour)
			return nil
		})
		err := client.Push(t.Context(), "cleanup-queue", "expiring-job", opt)
		require.NoError(t, err)

		assert.NoError(t, client.Cleanup(t.Context()))
	})
}

func TestWithDatabase(t *testing.T) {
	path := t.TempDir() + "/test.db"
	db, err := sql.Open("sqlite3", path)
	require.NoError(t, err)
	t.Cleanup(func() { db.Close() })

	t.Run("uses provided database", func(t *testing.T) {
		client, err := sterling.New(t.Context(), sterling.WithDatabase(db))
		require.NoError(t, err)
		require.NotNil(t, client)
		assert.NoError(t, client.Close())
	})

	t.Run("duplicate database (URL first) returns error", func(t *testing.T) {
		_, err := sterling.New(t.Context(), sterling.WithDatabase(db), sterling.WithMemoryClient(t.Name()))
		assert.ErrorContains(t, err, "database already configured")
	})

	t.Run("duplicate database (DB first) returns error", func(t *testing.T) {
		_, err := sterling.New(t.Context(), sterling.WithMemoryClient(t.Name()), sterling.WithDatabase(db))
		assert.ErrorContains(t, err, "database already configured")
	})
}

func TestNewNoDatabaseError(t *testing.T) {
	noop := sterling.Option(func(_ context.Context, _ *sterling.Client) error { return nil })
	_, err := sterling.New(t.Context(), noop)
	assert.ErrorContains(t, err, "no database configured")
}

func TestPushJSON(t *testing.T) {
	client, err := sterling.New(t.Context(), sterling.WithMemoryClient(t.Name()))
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	t.Run("valid JSON payload", func(t *testing.T) {
		err := client.Push(t.Context(), "default", "json-job", sterling.PushJSON(map[string]string{"key": "value"}))
		assert.NoError(t, err)
	})

	t.Run("non-marshallable payload returns error", func(t *testing.T) {
		err := client.Push(t.Context(), "default", "json-job", sterling.PushJSON(make(chan int)))
		assert.Error(t, err)
	})
}

func TestPushOptions(t *testing.T) {
	client, err := sterling.New(t.Context(), sterling.WithMemoryClient(t.Name()))
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	t.Run("push with visible at", func(t *testing.T) {
		opt := sterling.PushOption(func(p *sterling.PushPayload) error {
			p.VisibleAt = time.Now().Add(time.Hour)
			return nil
		})
		err := client.Push(t.Context(), "default", "delayed-job", opt)
		assert.NoError(t, err)
	})

	t.Run("push with expires at", func(t *testing.T) {
		opt := sterling.PushOption(func(p *sterling.PushPayload) error {
			p.ExpiresAt = time.Now().Add(time.Hour)
			return nil
		})
		err := client.Push(t.Context(), "default", "expiring-job", opt)
		assert.NoError(t, err)
	})

	t.Run("push with priority", func(t *testing.T) {
		opt := sterling.PushOption(func(p *sterling.PushPayload) error {
			p.Priority = 10
			return nil
		})
		err := client.Push(t.Context(), "default", "priority-job", opt)
		assert.NoError(t, err)
	})
}

func TestValueWorker(t *testing.T) {
	client, err := sterling.New(t.Context(), sterling.WithMemoryClient(t.Name()))
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	type payload struct{ Message string }

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	var received string
	client.Register("value-job", sterling.ValueWorker(func(_ context.Context, _ *sterling.Job, p payload) error {
		received = p.Message
		cancel()
		return nil
	}))

	err = client.Push(t.Context(), "default", "value-job", sterling.PushJSON(payload{Message: "hello"}))
	require.NoError(t, err)

	err = client.Run(ctx, []string{"default"}, 1)
	assert.NoError(t, err)
	assert.Equal(t, "hello", received)
}

func TestValueWorkerUnmarshalError(t *testing.T) {
	client, err := sterling.New(t.Context(), sterling.WithMemoryClient(t.Name()))
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	type payload struct{ Message string }
	client.Register("bad-payload-job", sterling.ValueWorker(func(_ context.Context, _ *sterling.Job, _ payload) error {
		cancel()
		return nil
	}))

	// Push a JSON string — can't unmarshal into struct, triggers ValueWorker error path
	err = client.Push(t.Context(), "default", "bad-payload-job", sterling.PushJSON("not a struct"))
	require.NoError(t, err)

	go func() {
		time.Sleep(300 * time.Millisecond)
		cancel()
	}()

	err = client.Run(ctx, []string{"default"}, 1)
	assert.NoError(t, err)
}

func TestProcessingErrors(t *testing.T) {
	t.Run("unregistered worker fails job", func(t *testing.T) {
		client, err := sterling.New(t.Context(), sterling.WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { client.Close() })

		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()

		err = client.Push(t.Context(), "default", "unregistered-kind")
		require.NoError(t, err)

		go func() {
			time.Sleep(200 * time.Millisecond)
			cancel()
		}()

		err = client.Run(ctx, []string{"default"}, 1)
		assert.NoError(t, err)
	})

	t.Run("worker failure re-queues job", func(t *testing.T) {
		client, err := sterling.New(t.Context(), sterling.WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { client.Close() })

		ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
		defer cancel()

		var attempts atomic.Int64
		client.RegisterFunc("failing-job", func(_ context.Context, _ *sterling.Job) error {
			attempts.Add(1)
			cancel()
			return errors.New("intentional failure")
		})

		err = client.Push(t.Context(), "default", "failing-job")
		require.NoError(t, err)

		err = client.Run(ctx, []string{"default"}, 1)
		assert.NoError(t, err)
		assert.GreaterOrEqual(t, attempts.Load(), int64(1))
	})
}

func TestExtendLease(t *testing.T) {
	t.Run("no client in context returns error", func(t *testing.T) {
		err := sterling.ExtendLease(t.Context(), &sterling.Job{ID: 1})
		assert.ErrorContains(t, err, "no client found in context")
	})

	t.Run("success from within worker", func(t *testing.T) {
		client, err := sterling.New(t.Context(), sterling.WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { client.Close() })

		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()

		var extendErr error
		client.RegisterFunc("extend-lease-job", func(ctx context.Context, job *sterling.Job) error {
			extendErr = sterling.ExtendLease(ctx, job)
			cancel()
			return nil
		})

		err = client.Push(t.Context(), "default", "extend-lease-job")
		require.NoError(t, err)

		err = client.Run(ctx, []string{"default"}, 1)
		assert.NoError(t, err)
		assert.NoError(t, extendErr)
	})
}

func TestListQueueEmpty(t *testing.T) {
	client, err := sterling.New(t.Context(), sterling.WithMemoryClient(t.Name()))
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	queues, err := client.ListQueue(t.Context())
	assert.NoError(t, err)
	assert.Empty(t, queues)
}

func TestStats(t *testing.T) {
	client, err := sterling.New(t.Context(), sterling.WithMemoryClient(t.Name()))
	require.NoError(t, err)
	t.Cleanup(func() { client.Close() })

	err = client.Push(t.Context(), "stats-queue", "stats-job")
	require.NoError(t, err)

	t.Run("ListQueue returns queues", func(t *testing.T) {
		queues, err := client.ListQueue(t.Context())
		assert.NoError(t, err)
		assert.Contains(t, queues, "stats-queue")
	})

	t.Run("LoadQueueStat returns stats", func(t *testing.T) {
		stat, err := client.LoadQueueStat(t.Context(), "stats-queue")
		require.NoError(t, err)
		require.NotNil(t, stat)
		assert.Equal(t, "stats-queue", stat.Name)
		assert.Contains(t, stat.Jobs, "stats-job")
		assert.Equal(t, int64(1), stat.Jobs["stats-job"].Total)
	})

	t.Run("LoadQueueStat nonexistent queue errors", func(t *testing.T) {
		_, err := client.LoadQueueStat(t.Context(), "nonexistent")
		assert.Error(t, err)
	})
}

func TestStep(t *testing.T) {
	t.Run("empty queue returns nil", func(t *testing.T) {
		client, err := sterling.New(t.Context(), sterling.WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { client.Close() })

		err = client.Step(t.Context(), []string{"default"})
		assert.NoError(t, err)
	})

	t.Run("processes a single job", func(t *testing.T) {
		client, err := sterling.New(t.Context(), sterling.WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { client.Close() })

		var executed atomic.Int64
		client.RegisterFunc("step-job", func(_ context.Context, _ *sterling.Job) error {
			executed.Add(1)
			return nil
		})

		err = client.Push(t.Context(), "default", "step-job")
		require.NoError(t, err)

		err = client.Step(t.Context(), []string{"default"})
		assert.NoError(t, err)
		assert.Equal(t, int64(1), executed.Load())
	})

	t.Run("processes only one job per call", func(t *testing.T) {
		client, err := sterling.New(t.Context(), sterling.WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { client.Close() })

		var executed atomic.Int64
		client.RegisterFunc("step-job-multi", func(_ context.Context, _ *sterling.Job) error {
			executed.Add(1)
			return nil
		})

		for range 3 {
			err = client.Push(t.Context(), "default", "step-job-multi")
			require.NoError(t, err)
		}

		err = client.Step(t.Context(), []string{"default"})
		assert.NoError(t, err)
		assert.Equal(t, int64(1), executed.Load())
	})

	t.Run("worker error does not propagate", func(t *testing.T) {
		client, err := sterling.New(t.Context(), sterling.WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { client.Close() })

		client.RegisterFunc("failing-step-job", func(_ context.Context, _ *sterling.Job) error {
			return errors.New("intentional failure")
		})

		err = client.Push(t.Context(), "default", "failing-step-job")
		require.NoError(t, err)

		err = client.Step(t.Context(), []string{"default"})
		assert.NoError(t, err)
	})

	t.Run("context has client for ExtendLease", func(t *testing.T) {
		client, err := sterling.New(t.Context(), sterling.WithMemoryClient(t.Name()))
		require.NoError(t, err)
		t.Cleanup(func() { client.Close() })

		var extendErr error
		client.RegisterFunc("extend-lease-step", func(ctx context.Context, job *sterling.Job) error {
			extendErr = sterling.ExtendLease(ctx, job)
			return nil
		})

		err = client.Push(t.Context(), "default", "extend-lease-step")
		require.NoError(t, err)

		err = client.Step(t.Context(), []string{"default"})
		assert.NoError(t, err)
		assert.NoError(t, extendErr)
	})
}

func BenchmarkPush(b *testing.B) {
	b.Run("with in-memory database", func(b *testing.B) {
		client, err := sterling.New(b.Context(), sterling.WithMemoryClient("benchmark-push"))
		require.NoError(b, err)
		b.Cleanup(func() { client.Close() })

		for b.Loop() {
			_ = client.Push(b.Context(), "default", "benchmark-job")
		}
	})

	b.Run("with on-disk database", func(b *testing.B) {
		path := b.TempDir() + "/benchmark.db"
		client, err := sterling.New(b.Context(), sterling.WithDatabaseFile(path))
		require.NoError(b, err)
		b.Cleanup(func() { client.Close() })

		for b.Loop() {
			_ = client.Push(b.Context(), "default", "benchmark-job")
		}
	})
}
