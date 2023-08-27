package tridb

import (
	"bytes"
	"fmt"
)

type keydir struct {
	ht *keydirHashtable
	l  *keydirOrderedList
}

type keydirItem struct {
	key            []byte
	position       *rowPosition
	isDeleted      bool // tombstone record for open adressing
	previous, next *keydirItem
}

type rowPosition struct{ Offset, Size int }

func newKeydir(length int) *keydir {
	return &keydir{ht: newKeydirHashtable(length), l: &keydirOrderedList{}}
}

func (kd *keydir) set(key []byte, position *rowPosition) {
	item := &keydirItem{key: key, position: position}

	// Delete from ordered list if already exists
	if existingItem := kd.ht.get(key); existingItem != nil {
		kd.l.remove(existingItem)
	}

	// Record in ordered list
	kd.l.push(item)

	// Resize hashtable if needed
	if kd.ht.loadFactor() > 0.5 {
		kd.ht.resize(len(kd.ht.entries) * 2)
	}

	// Set item in hashtable
	kd.ht.set(item)
}

func (kd *keydir) delete(key []byte) {
	item := kd.ht.get(key)
	if item == nil {
		return
	}

	// Remove item from ordered list
	kd.l.remove(item)

	// Delete item in hash table and downsize if possible
	kd.ht.delete(key)
	if kd.ht.loadFactor() < 0.125 {
		kd.ht.resize(len(kd.ht.entries) / 2)
	}
}

type keydirHashtable struct {
	count   int
	entries []*keydirItem
}

func newKeydirHashtable(length int) *keydirHashtable {
	if length <= 0 {
		length = 1024
	}
	return &keydirHashtable{
		entries: make([]*keydirItem, length),
	}
}

func hashFNV1a(key []byte) uint64 {
	const offset, prime = uint64(14695981039346656037), uint64(1099511628211)
	hash := offset
	for _, char := range key {
		hash *= prime
		hash ^= uint64(char)
	}
	return hash
}

func (ht *keydirHashtable) hash(key []byte) int { return int(hashFNV1a(key) % uint64(len(ht.entries))) }

func (ht *keydirHashtable) loadFactor() float64 { return float64(ht.count) / float64(len(ht.entries)) }

func (ht *keydirHashtable) resize(length int) {
	newHT := newKeydirHashtable(length)
	for _, entry := range ht.entries {
		if entry == nil {
			continue
		}
		newHT.set(entry)
	}
	*ht = *newHT
}

func (ht *keydirHashtable) set(item *keydirItem) {
	index := ht.hash(item.key)
	if ht.count >= len(ht.entries) {
		err := fmt.Errorf("hashtable overflow: %d/%d", ht.count, len(ht.entries))
		panic(err)
	}

	isNew := true
	for ; ; index++ {
		if index >= len(ht.entries) {
			index = 0
		}
		existingItem := ht.entries[index]
		if existingItem == nil {
			break // found available index
		}
		isSameKey := bytes.Equal(existingItem.key, item.key)
		if isSameKey && !existingItem.isDeleted {
			isNew = false
		}
		if isSameKey {
			break // found existing index
		}
		continue // Go to next index (tombstone or different key)
	}
	ht.entries[index] = item
	if isNew {
		ht.count++
	}
}

func (ht *keydirHashtable) delete(key []byte) {
	for index := ht.hash(key); true; index++ {
		if index >= len(ht.entries) {
			index = 0
		}
		item := ht.entries[index]
		if item == nil {
			return
		} else if bytes.Equal(key, item.key) && !item.isDeleted {
			ht.count--            // Decrease counter
			item.isDeleted = true // Set tombstone record
			return
		}
	}
}

func (ht *keydirHashtable) get(key []byte) *keydirItem {
	for index := ht.hash(key); true; index++ {
		if index >= len(ht.entries) {
			index = 0
		}
		item := ht.entries[index]
		if item == nil {
			break // key not found
		} else if bytes.Equal(key, item.key) && !item.isDeleted {
			return item
		}
	}
	return nil
}

type keydirOrderedList struct {
	first, last *keydirItem
}

func (l *keydirOrderedList) push(item *keydirItem) {
	item.previous = l.last
	if l.first == nil {
		l.first = item // set first item if needed
	}
	if l.last != nil {
		l.last.next = item // set previous item's next item (if not first item)
	}
	l.last = item // set last item to this
}

func (l *keydirOrderedList) remove(item *keydirItem) {
	if item.previous == nil {
		l.first = item.next
	} else {
		item.previous.next = item.next
	}
	if item.next == nil {
		l.last = item.previous
	} else {
		item.next.previous = item.previous
	}
}

type keydirCursor struct {
	l       *keydirOrderedList
	current *keydirItem
}

func (kd *keydir) cursor() *keydirCursor      { return &keydirCursor{l: kd.l, current: kd.l.last} }
func (c *keydirCursor) first() *keydirItem    { c.current = c.l.first; return c.current }
func (c *keydirCursor) last() *keydirItem     { c.current = c.l.last; return c.current }
func (c *keydirCursor) previous() *keydirItem { c.current = c.current.previous; return c.current }
func (c *keydirCursor) next() *keydirItem     { c.current = c.current.next; return c.current }
