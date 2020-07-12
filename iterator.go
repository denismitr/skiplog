package skiplog

type Iterator interface {
	Next() bool
	Offset() int64
	Entry() string
}

type UnboundIterator struct {
	current *node
	sl      *SkipLog
	offset  int64
	entry   string
}

func (i *UnboundIterator) Offset() int64 {
	return i.offset
}

func (i *UnboundIterator) Entry() string {
	return i.entry
}

func (i *UnboundIterator) Next() bool {
	i.sl.mu.RLock()
	defer i.sl.mu.RUnlock()

	if i.current == nil {
		return false
	}

	i.current = i.current.next[0]
	return true
}

type BoundIterator struct {
	UnboundIterator
	uBound  int64
	lBound  int64
}

func (i *BoundIterator) Next() bool {
	i.sl.mu.RLock()
	defer i.sl.mu.RUnlock()

	if i.current == nil {
		return false
	}

	if i.current.hasNextAtLevelLTE(0, i.uBound) {
		i.current = i.current.next[0]
		i.offset = i.current.offset
		i.entry = i.current.entry
		return true
	}

	return false
}