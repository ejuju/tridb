package tridb

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"strconv"
)

type Format interface {
	Encode(row *Row) ([]byte, error)
	DecodeFrom(bufr *bufio.Reader, row *Row) (int, error)
}

func FormatFromString(s string) Format {
	var format Format
	switch s {
	case "text", "txt", "t":
		format = DefaultTextEncoding
	case "text-auto-length", "auto-length", "auto":
		format = DefaultTextAutoLengthEncoding
	}
	return format
}

var DefaultFormat = DefaultTextAutoLengthEncoding

// Plain-text, easy to edit and read.
// Row data is written as plain-text and delimited using sentinel characters,
// consequently, the key and value should not include their corresponding suffix.
type TextAutoLengthEncoding struct {
	OpSuffix    byte
	KeySuffix   byte
	ValueSuffix byte
}

var DefaultTextAutoLengthEncoding = &TextAutoLengthEncoding{
	OpSuffix:    ' ',
	KeySuffix:   ' ',
	ValueSuffix: '\n',
}

func (e *TextAutoLengthEncoding) Encode(row *Row) ([]byte, error) {
	// Ensure sentinel characters are not included in key / value
	if i := bytes.IndexByte(row.Key, e.KeySuffix); i != -1 {
		return nil, fmt.Errorf("key contains sentinel suffix %q at index %d", e.KeySuffix, i)
	}
	if i := bytes.IndexByte(row.Value, e.ValueSuffix); i != -1 {
		return nil, fmt.Errorf("value contains sentinel suffix %q at index %d", e.ValueSuffix, i)
	}

	out := []byte{row.Op, e.OpSuffix}

	out = append(out, row.Key...)
	out = append(out, e.KeySuffix)
	out = append(out, row.Value...)
	out = append(out, e.ValueSuffix)

	return out, nil
}

func (e *TextAutoLengthEncoding) DecodeFrom(bufr *bufio.Reader, row *Row) (int, error) {
	read := 0

	// Read op and suffix
	opAndSuffix := make([]byte, 2)
	n, err := io.ReadFull(bufr, opAndSuffix)
	read += n
	if err != nil {
		return read, fmt.Errorf("read op and suffix: %w", err)
	}
	row.Op = opAndSuffix[0]

	// Read key and suffix
	keyAndSuffix, err := bufr.ReadBytes(e.KeySuffix)
	read += len(keyAndSuffix)
	if err != nil {
		return read, fmt.Errorf("read key and suffix: %w", err)
	}
	row.Key = keyAndSuffix[:len(keyAndSuffix)-1]

	// Read value and suffix
	valueAndSuffix, err := bufr.ReadBytes(e.ValueSuffix)
	read += len(valueAndSuffix)
	if err != nil {
		return read, fmt.Errorf("read value and suffix: %w", err)
	}
	row.Value = valueAndSuffix[:len(valueAndSuffix)-1]

	// Check suffixes
	if suffix := opAndSuffix[1]; suffix != e.OpSuffix {
		return read, fmt.Errorf("got op suffix %q instead of %q", suffix, e.OpSuffix)
	}
	if suffix := keyAndSuffix[len(keyAndSuffix)-1]; suffix != e.KeySuffix {
		return read, fmt.Errorf("got key suffix %q instead of %q", suffix, e.KeySuffix)
	}
	if suffix := valueAndSuffix[len(valueAndSuffix)-1]; suffix != e.ValueSuffix {
		return read, fmt.Errorf("got value suffix %q instead of %q", suffix, e.ValueSuffix)
	}

	return read, nil
}

type TextEncoding struct {
	OpSuffix          byte
	KeyLengthSuffix   byte
	ValueLengthSuffix byte
	KeySuffix         byte
	ValueSuffix       byte
}

var DefaultTextEncoding = &TextEncoding{
	OpSuffix:          ' ',
	KeyLengthSuffix:   ' ',
	ValueLengthSuffix: ' ',
	KeySuffix:         ' ',
	ValueSuffix:       '\n',
}

func (e *TextEncoding) Encode(row *Row) ([]byte, error) {
	// Ensure key and value length do not overflow assigned bit size.
	if len(row.Key) > math.MaxUint16 {
		return nil, fmt.Errorf("key too long: %d bytes", len(row.Key))
	}
	if len(row.Value) > math.MaxUint32 {
		return nil, fmt.Errorf("value too long: %d bytes", len(row.Value))
	}

	out := []byte{row.Op, e.OpSuffix}

	out = append(out, strconv.FormatUint(uint64(len(row.Key)), 10)...)
	out = append(out, e.KeyLengthSuffix)
	out = append(out, strconv.FormatUint(uint64(len(row.Value)), 10)...)
	out = append(out, e.ValueLengthSuffix)

	out = append(out, row.Key...)
	out = append(out, e.KeySuffix)
	out = append(out, row.Value...)
	out = append(out, e.ValueSuffix)

	return out, nil
}

func (e *TextEncoding) DecodeFrom(bufr *bufio.Reader, row *Row) (int, error) {
	read := 0

	// Read op and suffix
	opAndSuffix := make([]byte, 2)
	n, err := io.ReadFull(bufr, opAndSuffix)
	read += n
	if err != nil {
		return read, fmt.Errorf("read op and suffix: %w", err)
	}
	row.Op = opAndSuffix[0]

	// Read key-length and suffix
	keyLengthAndSuffix, err := bufr.ReadBytes(e.KeyLengthSuffix)
	read += len(keyLengthAndSuffix)
	if err != nil {
		return read, fmt.Errorf("read key-length and suffix: %w", err)
	}
	keyLength, err := strconv.ParseUint(string(keyLengthAndSuffix[:len(keyLengthAndSuffix)-1]), 10, 16)
	if err != nil {
		return read, fmt.Errorf("parse key-length: %w", err)
	}

	// Read value-length and suffix
	valueLengthAndSuffix, err := bufr.ReadBytes(e.ValueLengthSuffix)
	read += len(valueLengthAndSuffix)
	if err != nil {
		return read, fmt.Errorf("read value-length and suffix: %w", err)
	}
	valueLength, err := strconv.ParseUint(string(valueLengthAndSuffix[:len(valueLengthAndSuffix)-1]), 10, 32)
	if err != nil {
		return read, fmt.Errorf("parse value-length: %w", err)
	}

	// Read key and suffix
	keyAndSuffix := make([]byte, keyLength+1)
	n, err = io.ReadFull(bufr, keyAndSuffix)
	read += n
	if err != nil {
		return read, fmt.Errorf("read key and suffix: %w", err)
	}
	row.Key = keyAndSuffix[:len(keyAndSuffix)-1]

	// Read value and suffix
	valueAndSuffix := make([]byte, valueLength+1)
	n, err = io.ReadFull(bufr, valueAndSuffix)
	read += n
	if err != nil {
		return read, fmt.Errorf("read value and suffix: %w", err)
	}
	row.Value = valueAndSuffix[:len(valueAndSuffix)-1]

	// Check suffixes
	if suffix := opAndSuffix[1]; suffix != e.OpSuffix {
		return read, fmt.Errorf("invalid op suffix %q instead of %q", suffix, e.OpSuffix)
	}
	if suffix := keyLengthAndSuffix[len(keyLengthAndSuffix)-1]; suffix != e.KeyLengthSuffix {
		return read, fmt.Errorf("invalid key-length suffix %q instead of %q", suffix, e.KeyLengthSuffix)
	}
	if suffix := valueLengthAndSuffix[len(valueLengthAndSuffix)-1]; suffix != e.ValueLengthSuffix {
		return read, fmt.Errorf("invalid value-length suffix %q instead of %q", suffix, e.ValueLengthSuffix)
	}
	if suffix := keyAndSuffix[len(keyAndSuffix)-1]; suffix != e.KeySuffix {
		return read, fmt.Errorf("invalid key suffix %q instead of %q", suffix, e.KeySuffix)
	}
	if suffix := valueAndSuffix[len(valueAndSuffix)-1]; suffix != e.ValueSuffix {
		return read, fmt.Errorf("invalid value suffix %q instead of %q", suffix, e.ValueSuffix)
	}

	return read, nil
}
