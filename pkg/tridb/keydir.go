package tridb

type keydir struct {
	hashtable   map[string]*keydirItem
	first, last *keydirItem
}

type keydirItem struct {
	key            []byte
	position       *rowPosition
	previous, next *keydirItem
}

type rowPosition struct{ Offset, Size int }

func newKeydir() *keydir { return &keydir{hashtable: make(map[string]*keydirItem)} }

func (kd *keydir) set(key []byte, position *rowPosition) {
	item := &keydirItem{key: key, position: position, previous: kd.last}
	if kd.first == nil && kd.last == nil {
		kd.first, kd.last = item, item // first item
	}
	currentLast := kd.last
	currentLast.next = item          // set next item on current last item
	kd.last = item                   // replace last item with new item
	kd.remove(key)                   // delete previously defined key (if needed)
	kd.hashtable[string(key)] = item // set item in hashtable
}

func (kd *keydir) remove(key []byte) {
	item, ok := kd.hashtable[string(key)]
	if !ok {
		return
	}
	if item.previous == nil {
		kd.first = item.next
	} else {
		item.previous.next = item.next
	}
	if item.next == nil {
		kd.last = item.previous
	} else {
		item.next.previous = item.previous
	}
	delete(kd.hashtable, string(key))
}

func (kd *keydir) count() int { return len(kd.hashtable) }

func (kd *keydir) get(key []byte) *rowPosition {
	item, ok := kd.hashtable[string(key)]
	if !ok {
		return nil
	}
	return item.position
}

type keydirCursor struct {
	kd      *keydir
	current *keydirItem
}

func (kd *keydir) cursor() *keydirCursor      { return &keydirCursor{kd: kd, current: kd.last} }
func (c *keydirCursor) first() *keydirItem    { c.current = c.kd.first; return c.current }
func (c *keydirCursor) last() *keydirItem     { c.current = c.kd.last; return c.current }
func (c *keydirCursor) previous() *keydirItem { c.current = c.current.previous; return c.current }
func (c *keydirCursor) next() *keydirItem     { c.current = c.current.next; return c.current }
