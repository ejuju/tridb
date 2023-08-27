package tridb

type Keydir interface {
	Set(key []byte, position *RowPosition)
	Delete(key []byte)
	Count() int
	Get(key []byte) *RowPosition
	Walk(opts *WalkOptions, do func(key []byte, position *RowPosition) error) error
}

type RowPosition struct{ Offset, Size int }

type WalkOptions struct {
	Prefix  []byte // only walk over keys with the given prefix
	Reverse bool   // reverse lexicographical order
}

const (
	// All printable ASCII characters [32 to 126]
	minAllowedKeyChar  uint8 = ' '
	maxAllowedKeyChar  uint8 = '~'
	numAllowedKeyChars int   = 1 + int(maxAllowedKeyChar) - int(minAllowedKeyChar)
)

func charToIndex(c byte) int { return int(c) - int(minAllowedKeyChar) }
func indexToChar(i int) byte { return byte(i) + minAllowedKeyChar }

type TrieKeydir struct {
	count int
	root  *trieKeydirNode
}

type trieKeydirNode struct {
	children [numAllowedKeyChars]*trieKeydirNode
	position *RowPosition
}

func NewTrieKeydir() *TrieKeydir { return &TrieKeydir{root: &trieKeydirNode{}} }

func (kd *TrieKeydir) Set(key []byte, position *RowPosition) {
	curr := kd.root
	for _, c := range key {
		i := charToIndex(c)
		if curr.children[i] == nil {
			curr.children[i] = &trieKeydirNode{}
		}
		curr = curr.children[i]
	}
	if curr.position == nil {
		kd.count++
	}
	curr.position = position
}

func (kd *TrieKeydir) Delete(key []byte) {
	curr := kd.root
	for _, c := range key {
		i := charToIndex(c)
		if curr.children[i] == nil {
			return // no-op if key not found
		}
		curr = curr.children[i]
	}
	if curr.position != nil {
		kd.count--
	}
	curr.position = nil
}

func (kd *TrieKeydir) Get(key []byte) *RowPosition {
	curr := kd.root
	for _, c := range key {
		i := charToIndex(c)
		if curr.children[i] == nil {
			return nil
		}
		curr = curr.children[i]
	}
	return curr.position
}

func (kd *TrieKeydir) Count() int { return kd.count }

func (kd *TrieKeydir) Walk(opts *WalkOptions, do func(key []byte, position *RowPosition) error) error {
	if opts == nil {
		opts = &WalkOptions{Prefix: []byte{}}
	} else if opts.Prefix == nil {
		opts.Prefix = []byte{}
	}
	curr := kd.root
	for _, c := range opts.Prefix {
		i := charToIndex(c)
		if curr.children[i] == nil {
			return nil
		}
		curr = curr.children[i]
	}
	return curr.walk(opts.Reverse, opts.Prefix, do)
}

func (kdn *trieKeydirNode) walk(reverse bool, prefix []byte, do func(key []byte, position *RowPosition) error) error {
	if kdn.position != nil {
		err := do(prefix, kdn.position)
		if err != nil {
			return err
		}
	}

	// Walk children in reverse order
	if reverse {
		for i := len(kdn.children) - 1; i >= 0; i-- {
			child := kdn.children[i]
			if child == nil {
				continue
			}
			err := child.walk(reverse, append(prefix, indexToChar(i)), do)
			if err != nil {
				return err
			}
		}
		return nil
	}

	// Walk children in order
	for i := 0; i < len(kdn.children); i++ {
		child := kdn.children[i]
		if child == nil {
			continue
		}
		err := child.walk(reverse, append(prefix, indexToChar(i)), do)
		if err != nil {
			return err
		}
	}
	return nil
}

type OrderedMap struct {
	m           map[string]*orderedMapItem
	first, last *orderedMapItem
}

type orderedMapItem struct {
	key            []byte
	position       *RowPosition
	previous, next *orderedMapItem
}

func NewOrderedMap() *OrderedMap {
	return &OrderedMap{m: make(map[string]*orderedMapItem)}
}

func (kd *OrderedMap) Set(key []byte, position *RowPosition) {
	item := &orderedMapItem{key: key, position: position, previous: kd.last}
	kd.m[string(key)] = item

	// Update linked list
	if kd.first == nil && kd.last == nil {
		kd.first, kd.last = item, item // first item
	}
	kd.last.next = item // set next item on current last item
	kd.last = item      // replace last item with new item
}

func (kd *OrderedMap) Delete(key []byte) {
	item, ok := kd.m[string(key)]
	if !ok {
		return
	}
	delete(kd.m, string(key))

	// Update linked list
	if item.previous != nil {
		item.previous.next = item.next // replace next item for previous item if not first
	}
	if item.next != nil {
		item.next.previous = item.previous // replace previous item for next item if not last
	}
}

func (kd *OrderedMap) Count() int { return len(kd.m) }

func (kd *OrderedMap) Get(key []byte) *RowPosition {
	item, ok := kd.m[string(key)]
	if !ok {
		return nil
	}
	return item.position
}

func (kd *OrderedMap) Walk(opts *WalkOptions, do func(key []byte, position *RowPosition) error) error {
	if opts == nil {
		opts = &WalkOptions{Prefix: []byte{}}
	} else if opts.Prefix == nil {
		opts.Prefix = []byte{}
	}
	if kd.first == nil {
		return nil
	}
	c := keydirCursor{kd: kd}
	for item := c.last(); item != nil; item = c.next() {
		err := do(item.key, item.position)
		if err != nil {
			return err
		}
	}
	return nil
}

type keydirCursor struct {
	kd      *OrderedMap
	current *orderedMapItem
}

func (c *keydirCursor) first() *orderedMapItem {
	c.current = c.kd.first
	return c.current
}

func (c *keydirCursor) last() *orderedMapItem {
	c.current = c.kd.last
	return c.current
}

func (c *keydirCursor) previous() *orderedMapItem {
	c.current = c.current.previous
	return c.current
}

func (c *keydirCursor) next() *orderedMapItem {
	c.current = c.current.previous
	return c.current
}
