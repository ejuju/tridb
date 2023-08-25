package tridb

import (
	"testing"
)

func TestKeydir(t *testing.T) {
	t.Run("iterate over keys in lexicographical order", func(t *testing.T) {
		keydir := &keydir{root: &keydirNode{}}
		keydir.set([]byte("c"), &RowPosition{})
		keydir.set([]byte("b"), &RowPosition{})
		keydir.set([]byte("a"), &RowPosition{})
		wantKeys := [][]byte{[]byte("a"), []byte("b"), []byte("c")}
		gotKeys := [][]byte{}
		_ = keydir.walk(nil, func(key []byte, position *RowPosition) error {
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
		keydir := &keydir{root: &keydirNode{}}
		keydir.set([]byte("a"), &RowPosition{})
		keydir.set([]byte("b"), &RowPosition{})
		keydir.set([]byte("c"), &RowPosition{})
		wantKeys := [][]byte{[]byte("c"), []byte("b"), []byte("a")}
		gotKeys := [][]byte{}
		_ = keydir.walk(&WalkOptions{Reverse: true}, func(key []byte, position *RowPosition) error {
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
