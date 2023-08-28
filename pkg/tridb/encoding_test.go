package tridb

import (
	"bytes"
	"testing"
)

func TestEncoding(t *testing.T) {
	tests := []struct {
		desc    string
		row     *Row
		encoded []byte
	}{
		{
			desc:    "encode set row",
			row:     &Row{Key: []byte("Key"), Value: []byte("Value")},
			encoded: []byte{opSet, 3, 0, 0, 0, 5, 'K', 'e', 'y', 'V', 'a', 'l', 'u', 'e'},
		},
		{
			desc:    "encode set row with empty value",
			row:     &Row{Key: []byte("Key"), Value: nil},
			encoded: []byte{opSet, 3, 0, 0, 0, 0, 'K', 'e', 'y'},
		},
		{
			desc:    "encode delete row",
			row:     &Row{IsDeleted: true, Key: []byte("Key")},
			encoded: []byte{opDelete, 3, 0, 0, 0, 0, 'K', 'e', 'y'},
		},
	}

	for _, test := range tests {
		t.Run(test.desc, func(t *testing.T) {
			gotEncoded, err := test.row.Encode()
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(gotEncoded, test.encoded) {
				t.Fatalf("got encoded row %v instead of %v", gotEncoded, test.encoded)
			}

			gotDecoded := &Row{}
			n, err := gotDecoded.DecodeFrom(bytes.NewReader(test.encoded))
			if err != nil {
				t.Fatalf("decode: %s", err)
			}
			if n != len(test.encoded) {
				t.Fatalf("got decoding read size %d instead of %d", n, len(test.encoded))
			}
			isSameOp := gotDecoded.IsDeleted == test.row.IsDeleted
			isSameKey := bytes.Equal(gotDecoded.Key, test.row.Key)
			isSameValue := bytes.Equal(gotDecoded.Value, test.row.Value)
			if !isSameOp || !isSameKey || !isSameValue {
				t.Fatalf("got decoded row %+v instead of %+v", gotDecoded, test.row)
			}
		})
	}
}
