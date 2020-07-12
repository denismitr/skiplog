package skiplog

type node struct {
	next   [maxLevel]*node
	offset int64
	entry  string
	prev   *node
}

func (n *node) hasNextAtLevel(level int) bool {
	if level < 0 || level >= len(n.next) {
		return false
	}

	if n.next[level] == nil {
		return false
	}

	return true
}

func (n *node) hasNextAtLevelEqualTo(level int, offset int64) bool {
	if level < 0 || level >= len(n.next) || n.next[level] == nil {
		return false
	}

	if n.next[level].offset != offset {
		return false
	}

	return true
}

func (n *node) hasNextAtLevelLT(level int, offset int64) bool {
	if level < 0 || level >= len(n.next) || n.next[level] == nil {
		return false
	}

	if n.next[level].offset >= offset {
		return false
	}

	return true
}

func (n *node) hasNextAtLevelLTE(level int, offset int64) bool {
	if level < 0 || level >= len(n.next) || n.next[level] == nil {
		return false
	}

	if n.next[level].offset <= offset {
		return true
	}

	return false
}

func (n *node) hasNextAtLevelGTE(level int, offset int64) bool {
	if level < 0 || level >= len(n.next) || n.next[level] == nil {
		return false
	}

	if n.next[level].offset < offset {
		return false
	}

	return true
}

func (n *node) getNextAtLevel(level int) *node {
	if level < 0 || level >= len(n.next) {
		return nil
	}

	if n.next[level] == nil {
		return nil
	}

	return n.next[level]
}




