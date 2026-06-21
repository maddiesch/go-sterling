package sterling

import (
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
