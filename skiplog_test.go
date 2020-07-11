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

		assert.Equal(t, 5, tl.Len())

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
		sl.Insert(now.Add(6*time.Second).Unix(), "1006 - entry")

		assert.Equal(t, 6, sl.Len())

		e1, offset, err := sl.FirstGTE(now.Add(2*time.Second).Unix())
		assert.NoError(t, err)
		assert.Equal(t, now.Add(2*time.Second).Unix(), offset)
		assert.Equal(t, "1002 - entry", e1)

		e2, offset, err := sl.FirstGTE(now.Add(4*time.Second).Unix())
		assert.NoError(t, err)
		assert.Equal(t, now.Add(4*time.Second).Unix(), offset)
		assert.Equal(t, "1004 - entry", e2)

		e3, offset, err := sl.FirstGTE(now.Add(5*time.Second).Unix())
		assert.NoError(t, err)
		assert.Equal(t, now.Add(6*time.Second).Unix(), offset)
		assert.Equal(t, "1006 - entry", e3)
	})

	t.Run("heads-and-tails", func(t *testing.T) {
		tl := skiplog.New()
		tl.Insert(55, "55 - entry")
		tl.Insert(66, "66 - entry")
		tl.Insert(52, "52 - entry")
		tl.Insert(69, "69 - entry")
		tl.Insert(3, "3 - entry")
		tl.Insert(30, "30 - entry")
		tl.Insert(90, "90 - entry")

		assert.Equal(t, 7, tl.Len())

		n1, err := tl.Find(55)
		assert.NoError(t, err)
		assert.Equal(t, "55 - entry", n1)

		n2, err := tl.Find(69)
		assert.NoError(t, err)
		assert.Equal(t, "69 - entry", n2)

		n3, err := tl.Find(3)
		assert.NoError(t, err)
		assert.Equal(t, "3 - entry", n3)

		n4, err := tl.Find(66)
		assert.NoError(t, err)
		assert.Equal(t, "66 - entry", n4)

		n5, err := tl.Find(30)
		assert.NoError(t, err)
		assert.Equal(t, "30 - entry", n5)

		n6, err := tl.Find(90)
		assert.NoError(t, err)
		assert.Equal(t, "90 - entry", n6)

		n7, err := tl.Find(52)
		assert.NoError(t, err)
		assert.Equal(t, "52 - entry", n7)
	})
}

func TestRemoveEntries(t *testing.T) {
	entries := []struct{
		offset int64
		entry string
	}{
		{offset: 123, entry: "123 - entry"},
		{offset: 236, entry: "236 - entry"},
		{offset: 67, entry: "67 - entry"},
		{offset: 999, entry: "999 - entry"},
		{offset: 568, entry: "568 - entry"},
		{offset: 124, entry: "124 - entry"},
		{offset: 222, entry: "222 - entry"},
		{offset: 2223, entry: "2223 - entry"},
		{offset: 12, entry: "12 - entry"},
	}

	t.Run("insert and than remove all", func(t *testing.T) {
		tl := skiplog.New()

		for _, e := range entries {
			tl.Insert(e.offset, e.entry)
		}

		assert.Equal(t, len(entries), tl.Len())

		for _, e := range entries {
			err := tl.Remove(e.offset)
			assert.NoError(t, err)
		}

		assert.Equal(t, 0, tl.Len())

		entry, err := tl.Find(222)
		assert.Error(t, err)
		assert.Equal(t, skiplog.ErrSkipLogIsEmpty, err)
		assert.Equal(t, "", entry)
	})

	t.Run("insert and than remove some", func(t *testing.T) {
		tl := skiplog.New()

		for _, e := range entries {
			tl.Insert(e.offset, e.entry)
		}

		assert.Equal(t, len(entries), tl.Len())

		_ = tl.Remove(568)
		_ = tl.Remove(123)

		assert.Equal(t, len(entries) - 2, tl.Len())

		e1, err := tl.Find(222)
		assert.NoError(t, err)
		assert.Equal(t, "222 - entry", e1)

		e2, offset, err := tl.FirstGTE(123)
		assert.NoError(t, err)
		assert.Equal(t, int64(124), offset)
		assert.Equal(t, "124 - entry", e2)
	})
}


