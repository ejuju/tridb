package tridb

import (
	"bytes"
)

type keydir struct {
	ht         *keydirHashTable
	chronoList *keydirChronoList
}

type keydirItem struct {
	key                        []byte
	position                   *rowPosition
	chronoPrevious, chronoNext *keydirItem // for iterating over keys in chronological order
	bucketPrevious, bucketNext *keydirItem // for iterating over a other keys in the same bucket
}

type rowPosition struct{ offset, size int }

func newKeydir(numBuckets int) *keydir {
	return &keydir{ht: newKeydirHashtable(numBuckets), chronoList: &keydirChronoList{}}
}

func (kd *keydir) set(item *keydirItem) {
	if existingItem := kd.ht.get(item.key); existingItem != nil {
		kd.chronoList.deref(existingItem) // Delete previous version from chrono list
	}
	kd.chronoList.append(item) // append to chrono list
	kd.ht.set(item)            // add to hashtable bucket
}

// no-op if key not found
func (kd *keydir) delete(key []byte) {
	if item := kd.ht.get(key); item != nil {
		kd.chronoList.deref(item)
		kd.ht.delete(key)
	}
}

type keydirHashTable struct {
	numItems int
	buckets  []*keydirHashtableBucket
}

type keydirHashtableBucket struct {
	numItemsInBucket int
	first, last      *keydirItem
}

const minBuckets = 32 * 1024

func newKeydirHashtable(numBuckets int) *keydirHashTable {
	if numBuckets <= minBuckets {
		numBuckets = minBuckets
	}
	return &keydirHashTable{buckets: make([]*keydirHashtableBucket, numBuckets)}
}

func (ht *keydirHashTable) set(item *keydirItem) {
	ht.numItems++ // optimistic increment
	bucketIndex := ht.keyToBucketIndexByFNV1A(item.key)
	bucket := ht.buckets[bucketIndex]
	if bucket == nil {
		ht.buckets[bucketIndex] = &keydirHashtableBucket{first: item, last: item, numItemsInBucket: 1}
		return
	}
	bucket.numItemsInBucket++ // optimistic increment
	for existingItem := bucket.first; existingItem != nil; existingItem = existingItem.bucketNext {
		if bytes.Equal(existingItem.key, item.key) {
			ht.deleteItem(existingItem, bucketIndex, bucket)
		}
	}
	// Add to end of bucket
	beforeLast := bucket.last        // get item before later one (currently last)
	item.bucketPrevious = beforeLast // set later item's previous to current last
	beforeLast.bucketNext = item     // set current last next item to later item
	bucket.last = item               // swap global last
}

func (ht *keydirHashTable) delete(key []byte) {
	bucketIndex := ht.keyToBucketIndexByFNV1A(key)
	bucket := ht.buckets[bucketIndex]
	if bucket == nil {
		return
	}
	for existingItem := bucket.first; existingItem != nil; existingItem = existingItem.chronoNext {
		if bytes.Equal(existingItem.key, key) {
			ht.deleteItem(existingItem, bucketIndex, bucket)
			return
		}
	}
}

func (ht *keydirHashTable) deleteItem(existingItem *keydirItem, bucketIndex int, bucket *keydirHashtableBucket) {
	ht.numItems--
	bucket.numItemsInBucket--
	if bucket.numItemsInBucket == 0 {
		ht.buckets[bucketIndex] = nil // Remove whole bucket if no more keys left
	} else {
		ht.deref(existingItem, bucket) // Remove all references to this item in the bucket
	}
}

func (ht *keydirHashTable) get(key []byte) *keydirItem {
	bucket := ht.buckets[ht.keyToBucketIndexByFNV1A(key)]
	if bucket == nil {
		return nil
	}
	for bucketItem := bucket.first; bucketItem != nil; bucketItem = bucketItem.chronoNext {
		if bytes.Equal(bucketItem.key, key) {
			return bucketItem
		}
	}
	return nil
}

func (ht *keydirHashTable) deref(existingItem *keydirItem, bucket *keydirHashtableBucket) {
	if existingItem == bucket.first {
		bucket.first = existingItem.chronoNext
	} else {
		existingItem.chronoPrevious.chronoNext = existingItem.chronoNext
	}
	if existingItem == bucket.last {
		bucket.last = existingItem.chronoPrevious
	} else {
		existingItem.chronoNext.chronoPrevious = existingItem.chronoPrevious
	}
}

func (ht *keydirHashTable) keyToBucketIndexByFNV1A(key []byte) int {
	const offset, prime = uint64(14695981039346656037), uint64(1099511628211) // fnv-1a constants
	hash := offset
	for _, char := range key {
		hash *= prime
		hash ^= uint64(char)
	}
	index := int(hash % uint64(len(ht.buckets)))
	return index
}

func (ht *keydirHashTable) loadFactor() float64 {
	return float64(ht.numItems) / float64(len(ht.buckets))
}

type keydirChronoList struct {
	first, last *keydirItem
}

func (l *keydirChronoList) append(item *keydirItem) {
	item.chronoPrevious = l.last
	if l.first == nil {
		l.first = item // set first item if needed
	}
	if l.last != nil {
		l.last.chronoNext = item // set previous item's next item (if not first item)
	}
	l.last = item // set last item to this
}

func (l *keydirChronoList) deref(item *keydirItem) {
	if item.chronoPrevious == nil {
		l.first = item.chronoNext
	} else {
		item.chronoPrevious.chronoNext = item.chronoNext
	}
	if item.chronoNext == nil {
		l.last = item.chronoPrevious
	} else {
		item.chronoNext.chronoPrevious = item.chronoPrevious
	}
}
