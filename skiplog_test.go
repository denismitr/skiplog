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
		tl.Insert(1, "1 - entry")
		tl.Insert(984, "984 - entry")
		tl.Insert(1345, "1345 - entry")
		tl.Insert(50, "50 - entry")
		tl.Insert(1540, "1540 - entry")

		assert.Equal(t, 5, tl.Length())

		e3, err := tl.Find(984)
		assert.NoError(t, err)

		e1, err := tl.Find(1)
		assert.NoError(t, err)

		e4, err := tl.Find(1345)
		assert.NoError(t, err)

		e2, err := tl.Find(50)
		assert.NoError(t, err)

		e5, err := tl.Find(1540)
		assert.NoError(t, err)

		assert.Equal(t, "50 - entry", e2)
		assert.Equal(t, "1 - entry", e1)
		assert.Equal(t, "984 - entry", e3)
		assert.Equal(t, "1345 - entry", e4)
		assert.Equal(t, "1540 - entry", e5)
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


