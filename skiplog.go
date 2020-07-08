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

func (l *SkipLog) Append(offset uint64, command string) {
	//l.Lock()
	//defer l.Unlock()

	level := l.getLevel()

	if level > l.levels {
		level = l.levels + 1
		l.levels = level
	}

	n := &node{next: [maxLevel]*node{}, level: level, offset: offset, command: command}
	l.length++

	newHeads := true
	newTails := true

	if !l.IsEmpty() {
		newHeads = n.offset < l.heads[0].offset
		newTails = n.offset > l.tails[0].offset
	}

	normallyInserted := false
	if !newHeads && !newTails {
		normallyInserted = true
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
			if point <= level && (next == nil || next.offset > n.offset) {
				n.next[point] = next
				if current != nil {
					current.next[point] = next
				}

				if point == 0 {
					n.prev = current
					if next != nil {
						next.prev = n
					}
				}
			}

			if next != nil && next.offset <= n.offset {
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
		if newHeads || normallyInserted {
			if l.heads[i] == nil || l.heads[i].offset > offset {
				if i == 0 && l.heads[0] != nil {
					l.heads[0].prev = n
				}
				n.next[i] = l.heads[i]
			}

			// link the tails to the new node
			if n.next[i] == nil {
				l.tails[i] = n
			}

			adjusted = true
		}

		if newTails {
			// Places the new node after the very last node on this level
			// So the first node is not linked to itself

			if !newHeads {
				if l.tails[i] != nil {
					l.tails[i].next[i] = n
				}
				if i == 0 {
					n.prev = l.tails[0]
				}
				l.tails[i] = n
			}

			// Link the heads to the new node
			if l.heads[i] == nil || l.heads[i].offset > n.offset {
				l.heads[i] = n
			}

			adjusted = true
		}

		if !adjusted {
			break
		}
	}
}

func (l *SkipLog) entryPoint(offset uint64, level int) int {
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

func (l *SkipLog) find(offset uint64, gte bool) (string, uint64, error) {
	if l.IsEmpty() {
		return "", 0, ErrTransactionLogIsEmpty
	}

	point := l.entryPoint(offset, 0)

	current := l.heads[point]
	next := current

	if gte && current.offset > offset {
		return current.command, current.offset, nil
	}

	for {
		if current.offset == offset {
			return current.command, current.offset, nil
		}

		next = current.next[point]

		// Which direction to go next
		if next != nil && next.offset < offset {
			// Go right
			current = next
		} else {
			if point > 0 {
				if current.next[0] != nil && current.next[0].offset == offset {
					return current.next[0].command, current.next[0].offset, nil
				}
				point--
			} else {
				if gte && next != nil {
					return next.command, next.offset, nil
				}

				if current.next[0] != nil && current.next[0].offset == offset {
					return current.next[0].command, current.next[0].offset, nil
				}

				return "", 0, errors.Wrapf(ErrOffsetNotFound, "%d", offset)
			}
		}
	}
}

func (l *SkipLog) Find(offset uint64) (string, error) {
	l.RLock()
	defer l.RUnlock()

	command, _, err := l.find(offset, false)

	return command, err
}

func (l *SkipLog) FindGTE(offsetGte uint64) (string, uint64, error) {
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
