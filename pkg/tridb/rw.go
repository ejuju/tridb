package tridb

import (
	"bytes"
	"errors"
	"fmt"
	"os"
)

// ErrBreak is an error that can be used to abort a read-write transaction or exit a walk.
// It is only declared for convenience, it does nothing special.
var ErrBreak = errors.New("break")

// Read-write executes a read-write transaction.
//
// The transaction can be aborted by returning a non-nil error in the callback.
func (f *File) ReadWrite(do func(r *Reader, w *Writer) error) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Execute callback
	r, w := &Reader{f: f}, &Writer{}
	err := do(r, w)
	if err != nil {
		return err
	}

	// Write rows to file
	startOffset := f.woffset
	for _, row := range w.rows {
		// Encode row
		encoded, err := row.Encode()
		if err != nil {
			if f.woffset == startOffset {
				return fmt.Errorf("encode: %w", err)
			}
			truncErr := os.Truncate(f.fpath, int64(startOffset))
			if truncErr != nil {
				panic(fmt.Errorf("file corruption: %w: %w", err, truncErr))
			}
			panic(fmt.Errorf("memstate corruption: %w", err))
		}

		// Write row
		n, err := f.w.Write(encoded)
		f.woffset += n
		if err != nil {
			if f.woffset == startOffset {
				return fmt.Errorf("write: %w", err)
			}
			truncErr := os.Truncate(f.fpath, int64(startOffset))
			if truncErr != nil {
				panic(fmt.Errorf("file corruption: %w: %w", err, truncErr))
			}
			panic(fmt.Errorf("memstate corruption: %w", err))
		}

		// Update memstate
		switch row.Op {
		default:
			panic(fmt.Errorf("file corruption: unexpected op %q", row.Op))
		case OpSet:
			f.keydir.set(row.Key, &RowPosition{Offset: f.woffset - n, Size: n})
		case OpDelete:
			f.keydir.delete(row.Key)
		}
	}

	// Sync file
	err = f.w.Sync()
	if err != nil {
		panic(fmt.Errorf("sync: file corruption: %w", err))
	}

	return nil
}

// Note: In a read-only transaction,
// the returned error can only originate from the callback, therefore it can be ignored if the
// callback never fails (for example, when using `r.Has`, `r.Walk` or `r.Count`).
func (f *File) Read(do func(r *Reader) error) error {
	f.mu.RLock()
	defer f.mu.RUnlock()

	r := &Reader{f: f}
	return do(r)
}

type Writer struct{ rows []*Row }

func (w *Writer) Set(key, value []byte) {
	w.rows = append(w.rows, &Row{Op: OpSet, Key: key, Value: value})
}

func (w *Writer) Delete(key []byte) {
	w.rows = append(w.rows, &Row{Op: OpDelete, Key: key})
}

type Reader struct{ f *File }

func (r *Reader) Has(key []byte) bool { return r.f.keydir.get(key) != nil }

// Get returns the eventual value associated with the given key,
// if the key is not found, a nil value is returned.
// If an error is returned, it is internal (failed OS read or decoding).
func (r *Reader) Get(key []byte) ([]byte, error) {
	position := r.f.keydir.get(key)
	if position == nil {
		return nil, nil
	}
	row, err := r.readAndDecode(position)
	if err != nil {
		return nil, err
	}
	return row.Value, nil
}

func (r *Reader) Walk(opts *WalkOptions, do func(key []byte) error) error {
	return r.f.keydir.walk(opts, func(key []byte, _ *RowPosition) error { return do(key) })
}

func (r *Reader) WalkWithValue(opts *WalkOptions, do func(key, value []byte) error) error {
	return r.f.keydir.walk(opts, func(key []byte, position *RowPosition) error {
		row, err := r.readAndDecode(position)
		if err != nil {
			return err
		}
		return do(key, row.Value)
	})
}

func (r *Reader) Count(prefix []byte) int {
	count := 0
	r.f.keydir.walk(nil, func(key []byte, _ *RowPosition) error { count++; return nil })
	return count
}

func (r *Reader) readAndDecode(position *RowPosition) (*Row, error) {
	// Read row
	encodedRow := make([]byte, position.Size)
	_, err := r.f.r.ReadAt(encodedRow, int64(position.Offset))
	if err != nil {
		return nil, fmt.Errorf("read row: %w", err)
	}
	// Decode row
	row := &Row{}
	_, err = row.DecodeFrom(bytes.NewReader(encodedRow))
	if err != nil {
		return nil, fmt.Errorf("decode row: %w", err)
	}
	return row, nil
}
