package tridb

import (
	"testing"
)

func TestKeydir(t *testing.T) {
	t.Run("iterate over keys in lexicographical order", func(t *testing.T) {
		keydir := NewTrieKeydir()
		keydir.Set([]byte("c"), &RowPosition{})
		keydir.Set([]byte("b"), &RowPosition{})
		keydir.Set([]byte("a"), &RowPosition{})
		wantKeys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
		gotKeys := [][]byte{}
		_ = keydir.Walk(nil, func(key []byte, position *RowPosition) error {
			gotKeys = append(gotKeys, key)
			return nil
		})
		for i, wantKey := range wantKeys {
			if string(gotKeys[i]) != string(wantKey) {
				t.Fatalf("got key %q instead of %q at position %d", gotKeys[i], wantKey, i)
			}
		}
	})

	t.Run("iterate over keys in reverse order", func(t *testing.T) {
		keydir := NewTrieKeydir()
		keydir.Set([]byte("a"), &RowPosition{})
		keydir.Set([]byte("b"), &RowPosition{})
		keydir.Set([]byte("c"), &RowPosition{})
		wantKeys := [][]byte{[]byte("c"), []byte("b"), []byte("a")}
		gotKeys := [][]byte{}
		_ = keydir.Walk(&WalkOptions{Reverse: true}, func(key []byte, position *RowPosition) error {
			gotKeys = append(gotKeys, key)
			return nil
		})
		for i, wantKey := range wantKeys {
			if string(gotKeys[i]) != string(wantKey) {
				t.Fatalf("got key %q instead of %q at position %d", gotKeys[i], wantKey, i)
			}
		}
	})
}
