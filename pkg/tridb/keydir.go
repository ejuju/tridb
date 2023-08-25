package tridb

type keydir struct {
	root *keydirNode
}

type keydirNode struct {
	children [256]*keydirNode
	position *RowPosition
}

type RowPosition struct{ Offset, Size int }

func (km *keydir) set(key []byte, position *RowPosition) {
	curr := km.root
	for _, c := range key {
		if curr.children[c] == nil {
			curr.children[c] = &keydirNode{}
		}
		curr = curr.children[c]
	}
	curr.position = position
}

func (km *keydir) delete(key []byte) {
	curr := km.root
	for _, c := range key {
		if curr.children[c] == nil {
			return // no-op if key not found
		}
		curr = curr.children[c]
	}
	curr.position = nil
}

func (km *keydir) get(key []byte) *RowPosition {
	curr := km.root
	for _, c := range key {
		if curr.children[c] == nil {
			return nil
		}
		curr = curr.children[c]
	}
	return curr.position
}

type WalkOptions struct {
	Prefix  []byte // only walk over keys with the given prefix
	Reverse bool   // reverse lexicographical order
}

func (km *keydir) walk(opts *WalkOptions, do func(key []byte, position *RowPosition) error) error {
	if opts == nil {
		opts = &WalkOptions{Prefix: []byte{}}
	} else if opts.Prefix == nil {
		opts.Prefix = []byte{}
	}
	curr := km.root
	for _, c := range opts.Prefix {
		if curr.children[c] == nil {
			return nil
		}
		curr = curr.children[c]
	}
	return curr.walk(opts.Reverse, opts.Prefix, do)
}

func (n *keydirNode) walk(reverse bool, prefix []byte, do func(key []byte, position *RowPosition) error) error {
	if n.position != nil {
		err := do(prefix, n.position)
		if err != nil {
			return err
		}
	}

	// Walk in reverse lexicographical order
	if reverse {
		for c := len(n.children) - 1; c >= 0; c-- {
			child := n.children[c]
			if child == nil {
				continue
			}
			err := child.walk(reverse, append(prefix, byte(c)), do)
			if err != nil {
				return err
			}
		}
		return nil
	}

	// Walk in lexicographical order
	for c := 0; c < len(n.children); c++ {
		child := n.children[c]
		if child == nil {
			continue
		}
		err := child.walk(reverse, append(prefix, byte(c)), do)
		if err != nil {
			return err
		}
	}
	return nil
}