package tridb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
)

// Row holds data about a single database operation,
// more specifically, a 'set' or 'delete' operation.
// Rows are persisted to a file.
type Row struct {
	Op         byte
	Key, Value []byte
}

// Characters used to encode the type of write operations into a row.
const (
	OpSet    byte = '+'
	OpDelete byte = '-'
)

// Key/value length constraints.
const (
	MaxKeyLength   = math.MaxUint8  // Maximum allowed key-length.
	MaxValueLength = math.MaxUint32 // Maximum allowed value-length.
)

// Key/value length constrains errors.
var (
	ErrKeyTooLong   = errors.New("key too long")   // Key-length overflows uint8.
	ErrValueTooLong = errors.New("value too long") // Value-length overflows uint32.
)

// Validate reports an error if the row key and/or value is too long.
func (row *Row) Validate() error {
	if len(row.Key) > MaxKeyLength {
		return fmt.Errorf("%w: %d", ErrKeyTooLong, len(row.Key))
	}
	if len(row.Value) > MaxValueLength {
		return fmt.Errorf("%w: %d", ErrValueTooLong, len(row.Value))
	}
	return nil
}

// Encode returns the encoded row or an error if the row is not valid.
func (row *Row) Encode() ([]byte, error) {
	if err := row.Validate(); err != nil {
		return nil, err
	}

	// Write header (op, key-length and value-length)
	encoded := []byte{row.Op, uint8(len(row.Key))}
	encoded = binary.BigEndian.AppendUint32(encoded, uint32(len(row.Value)))

	// Write key and value
	encoded = append(encoded, row.Key...)
	encoded = append(encoded, row.Value...)

	return encoded, nil
}

// DecodeFrom decodes a row from the given reader into the caller.
// It reports the number of bytes read from the reader and an eventual error.
//
// Note: the caller is only mutated if no errors were encountered.
func (row *Row) DecodeFrom(r io.Reader) (int, error) {
	read := 0

	// Read header (op, key-length and value-length)
	header := [1 + 1 + 4]byte{}
	n, err := io.ReadFull(r, header[:])
	read += n
	if err != nil {
		return read, fmt.Errorf("read header: %w", err)
	}

	// Read key
	key := make([]byte, uint8(header[1]))
	n, err = io.ReadFull(r, key)
	read += n
	if err != nil {
		return read, fmt.Errorf("read key: %w", err)
	}

	// Read value
	value := make([]byte, binary.BigEndian.Uint32(header[2:]))
	n, err = io.ReadFull(r, value)
	read += n
	if err != nil {
		return read, fmt.Errorf("read value: %w", err)
	}

	row.Op = header[0]
	row.Key = key
	row.Value = value
	return read, nil
}
