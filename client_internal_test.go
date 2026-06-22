package sterling

import (
	"context"
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
