package skiplog

import (
	"github.com/pkg/errors"
	"skiplog/boolgen"
	"sync"
)

var ErrTransactionLogIsEmpty = errors.New("transaction log is empty")
var ErrOffsetNotFound = errors.New("offset not found")

const maxLevel = 25

type SkipLog struct {
	sync.RWMutex
	heads    [maxLevel]*node
	tails    [maxLevel]*node
	maxLevel int
	levels   int
	length   int
}

func New() *SkipLog {
	return &SkipLog{
		heads:    [maxLevel]*node{},
		tails:    [maxLevel]*node{},
		maxLevel: maxLevel,
		length:   0,
	}
}

func (l *SkipLog) Length() int {
	l.RLock()
	defer l.RUnlock()
	return l.length
}

func (l *SkipLog) Insert(offset int64, entry string) {
	l.Lock()
	defer l.Unlock()

	level := l.getLevel()

	if level > l.levels {
		level = l.levels + 1
		l.levels = level
	}

	newNode := &node{
		next: [maxLevel]*node{},
		level: level,
		offset: offset,
		entry: entry,
	}

	l.length++

	isHead := true
	isTail := true

	if l.heads[0] != nil && l.tails[0] != nil {
		isHead = newNode.offset < l.heads[0].offset
		isTail = newNode.offset > l.tails[0].offset
	}

	norHeadsNorTails := !isHead && !isTail
	if norHeadsNorTails {
		point := l.entryPoint(offset, level)
		var current *node
		next := l.heads[point]

		for {
			if current == nil {
				next = l.heads[point]
			} else {
				next = current.next[point]
			}

			// Connect node to next
			if point <= level && (next == nil || next.offset > newNode.offset) {
				newNode.next[point] = next
				if current != nil {
					current.next[point] = newNode
				}

				if point == 0 {
					newNode.prev = current
					if next != nil {
						next.prev = newNode
					}
				}
			}

			if next != nil && next.offset <= newNode.offset {
				current = next
			} else {
				point--
				if point < 0 {
					break
				}
			}
		}
	}

	for i := level; i >= 0; i-- {
		adjusted := false
		if isHead || norHeadsNorTails {
			if l.heads[i] == nil || l.heads[i].offset > offset {
				if i == 0 && l.heads[0] != nil {
					l.heads[0].prev = newNode
				}
				newNode.next[i] = l.heads[i]
			}

			// link the tails to the new node
			if newNode.next[i] == nil {
				l.tails[i] = newNode
			}

			adjusted = true
		}

		if isTail {
			// Places the new node after the very last node on this level
			// So the first node is not linked to itself

			if !isHead {
				if l.tails[i] != nil {
					l.tails[i].next[i] = newNode
				}
				if i == 0 {
					newNode.prev = l.tails[0]
				}
				l.tails[i] = newNode
			}

			// Link the heads to the new node
			if l.heads[i] == nil || l.heads[i].offset > newNode.offset {
				l.heads[i] = newNode
			}

			adjusted = true
		}

		if !adjusted {
			break
		}
	}
}

func (l *SkipLog) entryPoint(offset int64, level int) int {
	for i := l.levels; i >= 0; i-- {
		if l.heads[i] != nil && l.heads[i].offset <= offset || i < level {
			return i
		}
	}

	return 0
}

func (l *SkipLog) IsEmpty() bool {
	return l.heads[0] == nil
}

func (l *SkipLog) find(offset int64, gte bool) (string, int64, error) {
	if l.IsEmpty() {
		return "", 0, ErrTransactionLogIsEmpty
	}

	point := l.entryPoint(offset, 0)

	current := l.heads[point]
	next := current

	if gte && current.offset > offset {
		return current.entry, current.offset, nil
	}

	for {
		if current.offset == offset {
			return current.entry, current.offset, nil
		}

		next = current.next[point]

		// Which direction to go next
		if next != nil && next.offset < offset {
			// Go right
			current = next
		} else {
			if point > 0 {
				if current.next[0] != nil && current.next[0].offset == offset {
					return current.next[0].entry, current.next[0].offset, nil
				}
				point--
			} else {
				if gte && next != nil {
					return next.entry, next.offset, nil
				}

				if current.next[0] != nil && current.next[0].offset == offset {
					return current.next[0].entry, current.next[0].offset, nil
				}

				return "", 0, errors.Wrapf(ErrOffsetNotFound, "%d", offset)
			}
		}
	}
}

func (l *SkipLog) Find(offset int64) (string, error) {
	l.RLock()
	defer l.RUnlock()

	command, _, err := l.find(offset, false)

	return command, err
}

// FirstGTE - looks for the first node that is greater or equal to the given offset
func (l *SkipLog) FirstGTE(offsetGte int64) (string, int64, error) {
	l.RLock()
	defer l.RUnlock()

	return l.find(offsetGte, true)
}

func (l *SkipLog) getLevel() int {
	b := boolgen.New()
	var n int
	for b.Bool() && n < l.maxLevel {
		n++
	}
	return n
}
