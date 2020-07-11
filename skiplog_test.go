package skiplog_test

import (
	"github.com/stretchr/testify/assert"
	"skiplog"
	"testing"
	"time"
)

func TestSkipLog(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		tl := skiplog.New()
		tl.Insert(1, "foo eat bar")
		tl.Insert(984, "baz said boo")
		tl.Insert(1345, "foo died")
		tl.Insert(1540, "baz also died")

		assert.Equal(t, 4, tl.Length())

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

	t.Run("sequence", func(t *testing.T) {
		now := time.Now()

		sl := skiplog.New()
		sl.Insert(now.Unix(), "1000 - entry")
		sl.Insert(now.Add(1*time.Second).Unix(), "1001 - entry")
		sl.Insert(now.Add(2*time.Second).Unix(), "1002 - entry")
		sl.Insert(now.Add(3*time.Second).Unix(), "1003 - entry")
		sl.Insert(now.Add(4*time.Second).Unix(), "1004 - entry")

		assert.Equal(t, 5, sl.Length())

		e1, offset, err := sl.FirstGTE(now.Add(2*time.Second).Unix())
		assert.NoError(t, err)

		assert.Equal(t, now.Add(2*time.Second).Unix(), offset)
		assert.Equal(t, "1002 - entry", e1)
	})
}


