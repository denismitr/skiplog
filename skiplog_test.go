package skiplog_test

import (
	"github.com/stretchr/testify/assert"
	"skiplog"
	"testing"
)

func TestSkipLog(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		tl := skiplog.New()
		tl.Append(1, "foo eat bar")
		tl.Append(984, "baz said boo")
		tl.Append(1345, "foo died")
		tl.Append(1540, "baz also died")

		c2, err := tl.Find(984)
		assert.NoError(t, err)

		c1, err := tl.Find(1)
		assert.NoError(t, err)

		c3, err := tl.Find(1345)
		assert.NoError(t, err)

		assert.Equal(t, 4, tl.Length())
		assert.Equal(t, "baz said boo", c2)
		assert.Equal(t, "foo eat bar", c1)
		assert.Equal(t, "foo died", c3)
	})
}


