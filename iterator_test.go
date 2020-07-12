package skiplog_test

import (
	"github.com/stretchr/testify/assert"
	"skiplog"
	"testing"
)

func TestRangeIterator(t *testing.T) {
	entries := []struct {
		offset int64
		entry  string
	}{
		{offset: 12, entry: "12 - entry"},
		{offset: 67, entry: "67 - entry"},
		{offset: 123, entry: "123 - entry"},
		{offset: 124, entry: "124 - entry"},
		{offset: 222, entry: "222 - entry"},
		{offset: 236, entry: "236 - entry"},
		{offset: 568, entry: "568 - entry"},
		{offset: 999, entry: "999 - entry"},
		{offset: 2223, entry: "2223 - entry"},
	}

	t.Run("range", func(t *testing.T) {
		tl := skiplog.New()

		for _, e := range entries {
			tl.Insert(e.offset, e.entry)
		}

		iter, err := tl.Range(124, 568)
		assert.NoError(t, err)

		var hasNext bool
		hasNext = iter.Next()
		assert.True(t, hasNext)
		assert.Equal(t, int64(124), iter.Offset())
		assert.Equal(t, "124 - entry", iter.Entry())

		hasNext = iter.Next()
		assert.True(t, hasNext)
		assert.Equal(t, int64(222), iter.Offset())
		assert.Equal(t, "222 - entry", iter.Entry())

		hasNext = iter.Next()
		assert.True(t, hasNext)
		assert.Equal(t, int64(236), iter.Offset())
		assert.Equal(t, "236 - entry", iter.Entry())

		hasNext = iter.Next()
		assert.True(t, hasNext)
		assert.Equal(t, int64(568), iter.Offset())
		assert.Equal(t, "568 - entry", iter.Entry())
	})
}
