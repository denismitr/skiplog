package skiplog

import (
	"github.com/pkg/errors"
	"skiplog/boolgen"
	"sync"
)

var ErrSkipLogIsEmpty = errors.New("transaction log is empty")
var ErrOffsetNotFound = errors.New("offset not found")

const maxLevel = 25

type SkipLog struct {
	mu sync.RWMutex
	heads    [maxLevel]*node
	tails    [maxLevel]*node
	levels   int
	length   int
}

func New() *SkipLog {
	return &SkipLog{
		heads:    [maxLevel]*node{},
		tails:    [maxLevel]*node{},
		levels: 0,
		length:   0,
	}
}

func (l *SkipLog) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.length
}

func (l *SkipLog) Insert(offset int64, entry string) {
	l.mu.Lock()
	defer l.mu.Unlock()

	level := l.getLevel()

	if level > l.levels {
		level = l.levels + 1
		l.levels = level
	}

	newNode := &node{
		next: [maxLevel]*node{},
		offset: offset,
		entry: entry,
	}

	l.length++

	isNewHeads := l.isNewHeads(newNode)
	isNewTails := l.isNewTails(newNode)
	isNotNewHeadsOrTails := !isNewHeads && !isNewTails

	if isNotNewHeadsOrTails {
		point := l.entryLevel(offset, level)
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
		if isNewHeads || isNotNewHeadsOrTails {
			if l.heads[i] == nil || l.heads[i].offset > offset {
				if i == 0 && l.heads[0] != nil {
					l.heads[0].prev = newNode
				}
				newNode.next[i] = l.heads[i]
				l.heads[i] = newNode
			}

			// link the tails to the new node
			if newNode.next[i] == nil {
				l.tails[i] = newNode
			}

			continue
		}

		if isNewTails {
			// Places the new node after the very last node on this level
			// So the first node is not linked to itself

			if !isNewHeads {
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

			continue
		}
	}
}

func (l *SkipLog) entryLevel(offset int64, level int) int {
	for i := l.levels; i >= 0; i-- {
		if (l.heads[i] != nil && l.heads[i].offset <= offset) || i < level {
			return i
		}
	}

	return 0
}

func (l *SkipLog) IsEmpty() bool {
	l.mu.RLock()
	defer l.mu.Unlock()
	return l.empty()
}

func (l *SkipLog) empty() bool {
	return l.heads[0] == nil && l.tails[0] == nil
}

func (l *SkipLog) find(offset int64, gte bool) (string, int64, error) {
	if l.empty() {
		return "", 0, ErrSkipLogIsEmpty
	}

	level := l.entryLevel(offset, 0)

	current := l.heads[level]
	next := current

	if gte && current.offset >= offset {
		return current.entry, current.offset, nil
	}

	for {
		if current.offset == offset {
			return current.entry, current.offset, nil
		}

		next = current.getNextAtLevel(level)

		// Which direction to go next
		if next != nil && next.offset < offset {
			// Go right
			current = next
			continue
		}

		if level > 0 {
			if current.hasNextAtLevelEqualTo(0, offset) {
				return current.next[0].entry, current.next[0].offset, nil
			}
			level--
			continue
		}

		if gte && next != nil {
			return next.entry, next.offset, nil
		}

		if current.next[0] != nil && current.next[0].offset == offset {
			return current.next[0].entry, current.next[0].offset, nil
		}

		return "", 0, errors.Wrapf(ErrOffsetNotFound, "%d", offset)
	}
}

func (l *SkipLog) Find(offset int64) (string, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	command, _, err := l.find(offset, false)

	return command, err
}

// FirstGTE - looks for the first node that is greater or equal to the given offset
func (l *SkipLog) FirstGTE(offsetGte int64) (string, int64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	return l.find(offsetGte, true)
}

// Remove entry under offset from SkipLog
func (l *SkipLog) Remove(offset int64) error {
	if l.empty() {
		return ErrSkipLogIsEmpty
	}

	level := l.entryLevel(offset, 0)

	var current *node
	var next *node

	for {
		if current == nil {
			next = l.heads[level]
		} else {
			next = current.getNextAtLevel(level)
		}

		if next != nil && next.offset == offset {
			return l.removeNode(current, next, level)
		}

		if next != nil && next.offset < offset {
			current = next
		} else {
			level--
			if level < 0 {
				break
			}
		}
	}

	return nil
}

func (l *SkipLog) removeNode(current *node, next *node, level int) error {
	if current != nil {
		current.next[level] = next.next[level]
	}

	if level == 0 {
		if next.hasNextAtLevel(0) {
			next.getNextAtLevel(0).prev = current
		}
		l.length--
	}

	// Heads must be adjusted
	if l.heads[level] == next {
		l.heads[level] = next.next[level]
		// This was the highest node
		if l.heads[level] == nil {
			l.levels--
		}
	}

	// Tails must be adjusted
	if next.next[level] == nil {
		l.tails[level] = current
	}

	next.next[level] = nil

	return nil
}

func (l *SkipLog) getLevel() int {
	b := boolgen.New()
	var n int
	for b.Bool() && n < l.levels {
		n++
	}
	return n
}

func (l *SkipLog) isNewHeads(n *node) bool {
	if n == nil {
		return false
	}

	if l.heads[0] == nil {
		return true
	}

	return n.offset < l.heads[0].offset
}

func (l *SkipLog) isNewTails(n *node) bool {
	if n == nil {
		return false
	}

	if l.tails[0] == nil {
		return true
	}

	return n.offset > l.tails[0].offset
}
