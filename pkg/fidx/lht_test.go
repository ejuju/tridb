package fidx

import (
	"bytes"
	"testing"
)

func TestIndex(t *testing.T) {
	idx := NewLHTIndex(1)

	key1 := []byte("MyKey1")
	key2 := []byte("MyKey2")
	key3 := []byte("MyKey3")

	// Add 3 entries
	putAndAssertInsertionAndCount(t, idx, key1, Position{1}, 1)
	putAndAssertInsertionAndCount(t, idx, key2, Position{2}, 2)
	putAndAssertInsertionAndCount(t, idx, key3, Position{3}, 3)

	// Walk in chronological order (from oldest to latest)
	var gotOrderedKeys [][]byte
	wantOrderedKeys := [][]byte{key1, key2, key3}
	for row := idx.Oldest; row != nil; row = row.Next {
		gotOrderedKeys = append(gotOrderedKeys, row.Key)
	}
	assertOrder(t, gotOrderedKeys, wantOrderedKeys)

	// Walk in reverse chronological order (from latest to oldest)
	var gotReverseOrderedKeys [][]byte
	wantReverseOrderedKeys := [][]byte{key3, key2, key1}
	for row := idx.Latest; row != nil; row = row.Previous {
		gotReverseOrderedKeys = append(gotReverseOrderedKeys, row.Key)
	}
	assertOrder(t, gotReverseOrderedKeys, wantReverseOrderedKeys)

	// Delete entries
	deleteAndAssertDeletionAndCount(t, idx, key1, 2)
	deleteAndAssertDeletionAndCount(t, idx, key2, 1)
	deleteAndAssertDeletionAndCount(t, idx, key3, 0)
}

func putAndAssertInsertionAndCount(t *testing.T, idx *LHTIndex, key []byte, p Position, count int) {
	t.Helper()
	idx.Put(key, p)
	assertInsertion(t, idx, key, p)
	assertCount(t, idx, count)
}

func deleteAndAssertDeletionAndCount(t *testing.T, idx *LHTIndex, key []byte, count int) {
	t.Helper()
	idx.Delete(key)
	assertDeletion(t, idx, key)
	assertCount(t, idx, count)
}

func assertInsertion(t *testing.T, idx *LHTIndex, key []byte, p Position) {
	t.Helper()
	got := idx.Get(key)
	if got == nil {
		t.Fatalf("%q not found", key)
	} else if got.Position.Offset() != p.Offset() || got.Position.Size() != p.Size() {
		t.Fatalf("got position %v instead of %v", got, p)
	}
}

func assertDeletion(t *testing.T, idx *LHTIndex, key []byte) {
	t.Helper()
	got := idx.Get(key)
	if got != nil {
		t.Fatalf("deleted key %q found %v", key, got)
	}
}

func assertCount(t *testing.T, idx *LHTIndex, want int) {
	t.Helper()
	if got := idx.Count; got != want {
		t.Fatalf("got count %d instead of %d", got, want)
	}
}

func assertOrder(t *testing.T, gotKeys, wantKeys [][]byte) {
	t.Helper()
	if got, want := len(gotKeys), len(wantKeys); got != want {
		t.Fatalf("iterated over %d rows instead of %d", got, want)
	}
	for i, want := range wantKeys {
		got := gotKeys[i]
		if !bytes.Equal(got, want) {
			t.Fatalf("iterated in wrong order %q instead of %q", gotKeys, wantKeys)
		}
	}
}
