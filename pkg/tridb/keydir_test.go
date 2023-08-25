package tridb

import (
	"testing"
)

func TestKeydir(t *testing.T) {
	t.Run("iterate over keys in lexicographical order", func(t *testing.T) {
		keydir := &keydir{root: &keydirNode{}}
		keydir.set([]byte("1"), &RowPosition{})
		keydir.set([]byte("2"), &RowPosition{})
		keydir.set([]byte("10"), &RowPosition{})

		// Walk over all keys
		wantKeys := [][]byte{[]byte("1"), []byte("10"), []byte("2")}
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
}
