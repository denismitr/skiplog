package skiplog

type node struct {
	next [maxLevel]*node
	level int
	offset uint64
	command string
	prev *node
}

func (n *node) hasNextAt(i int) bool {
	if i < 0 || i >= len(n.next) {
		return false
	}

	if n.next[i] == nil {
		return false
	}

	return true
}

func (n *node) peakNextAt(i int) *node {
	if i < 0 || i >= len(n.next) {
		return nil
	}

	if n.next[i] == nil {
		return nil
	}

	return n.next[i]
}




