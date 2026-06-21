package sterling_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	sterling "github.com/maddiesch/go-sterling"
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
}
