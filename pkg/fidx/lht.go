package fidx

import "bytes"

type RowInfo struct {
	Key            []byte   // user-defined key
	Position       Position // position in file
	Next, Previous *RowInfo // neighbouring rows in chronological order
	nextInBucket   *RowInfo // internal state for hashtable
}

type Position [2]int           // Position holds the offset and size of a row in a file.
func (p Position) Offset() int { return p[0] }
func (p Position) Size() int   { return p[1] }

// LHTIndex is an ordered map implementation based on a linked hash table.
// Insertion order is maintained based on when keys where created.
type LHTIndex struct {
	Count          int
	Oldest, Latest *RowInfo
	buckets        []*RowInfo
}

func NewLHTIndex(numBuckets int) *LHTIndex {
	return &LHTIndex{buckets: make([]*RowInfo, numBuckets)}
}

func (idx *LHTIndex) Put(key []byte, p Position) {
	bucketIndex := idx.hashFNV1aIndex(key)
	root := idx.buckets[bucketIndex]
	var previousInBucket *RowInfo
	for row := root; row != nil; row, previousInBucket = row.nextInBucket, row {
		if bytes.Equal(row.Key, key) {
			row.Position = p
			return
		}
	}

	// Append new row to bucket
	idx.Count++
	row := &RowInfo{Key: key, Position: p}
	if previousInBucket == nil {
		idx.buckets[bucketIndex] = row
	} else {
		previousInBucket.nextInBucket = row
	}

	// Add to end of chronological order
	appendChronologically(idx, row)
}

func (idx *LHTIndex) Delete(key []byte) {
	bucketIndex := idx.hashFNV1aIndex(key)
	root := idx.buckets[bucketIndex]
	var previousInBucket *RowInfo
	for row := root; row != nil; row, previousInBucket = row.nextInBucket, row {
		if bytes.Equal(row.Key, key) {
			// Delete in bucket and decrement count
			idx.Count--
			if previousInBucket == nil {
				idx.buckets[bucketIndex] = row.nextInBucket
			} else {
				previousInBucket.nextInBucket = row.nextInBucket
			}
			unlinkChronologically(idx, row)
			return
		}
	}
}

func unlinkChronologically(idx *LHTIndex, row *RowInfo) {
	if row.Previous == nil {
		idx.Oldest = row.Next
	} else {
		row.Previous.Next = row.Next
	}
	if row.Next == nil {
		idx.Latest = row.Previous
	} else {
		row.Next.Previous = row.Previous
	}
}

func appendChronologically(idx *LHTIndex, row *RowInfo) {
	row.Previous = idx.Latest
	if idx.Oldest == nil || idx.Latest == nil {
		idx.Oldest = row
	} else {
		idx.Latest.Next = row
	}
	idx.Latest = row
}

func (idx *LHTIndex) Get(key []byte) *RowInfo {
	root := idx.buckets[idx.hashFNV1aIndex(key)]
	for row := root; row != nil; row = row.nextInBucket {
		if bytes.Equal(row.Key, key) {
			return row
		}
	}
	return nil
}

func (idx *LHTIndex) hashFNV1aIndex(key []byte) int {
	const offset, prime = uint64(14695981039346656037), uint64(1099511628211) // fnv-1a constants
	hash := offset
	for _, char := range key {
		hash *= prime
		hash ^= uint64(char)
	}
	index := int(hash % uint64(len(idx.buckets)))
	return index
}
