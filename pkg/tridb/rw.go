package tridb

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"os"
)

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
		encoded, err := f.fmt.Encode(row)
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
			f.km.set(row.Key, [2]int{f.woffset - n, n})
		case OpDelete:
			f.km.delete(row.Key)
		}
	}

	// Sync file
	err = f.w.Sync()
	if err != nil {
		panic(fmt.Errorf("sync: file corruption: %w", err))
	}

	return nil
}

// ErrBreak is an error that can be used to abort a read-write transaction or exit a walk.
// It is only declared for convenience, it does nothing special.
var ErrBreak = errors.New("break")

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

func (r *Reader) Has(key []byte) bool { return r.f.km.get(key) != nil }

func (r *Reader) Count(prefix []byte) int {
	count := 0
	r.f.km.walk(nil, func(key []byte, history [][2]int) error { count++; return nil })
	return count
}

func (r *Reader) Walk(opts *WalkOptions, do func(key []byte) error) error {
	return r.f.km.walk(opts, func(key []byte, _ [][2]int) error { return do(key) })
}

func (r *Reader) WalkWithVersions(opts *WalkOptions, do func(key []byte, history *Versions) error) error {
	return r.f.km.walk(opts, func(key []byte, history [][2]int) error {
		return do(key, &Versions{r: r, history: history})
	})
}

// Get returns the history for the given key,
// Even if the key is not found, an history (with length 0) is returned.
func (r *Reader) Get(key []byte) *Versions { return &Versions{r: r, history: r.f.km.get(key)} }

type Versions struct {
	r       *Reader
	history [][2]int
}

func (h *Versions) Length() int { return len(h.history) }

// Value returns the value in the key history at the given index.
// If the history is empty, the returned value is nil.
func (h *Versions) Value(i int) ([]byte, error) {
	if len(h.history) == 0 {
		return nil, nil
	}
	if i < 0 || i > len(h.history)-1 {
		return nil, fmt.Errorf("invalid history index %d (max %d)", i, len(h.history)-1)
	}

	// Read row
	position := h.history[i]
	encodedRow := make([]byte, position[1])
	_, err := h.r.f.r.ReadAt(encodedRow, int64(position[0]))
	if err != nil {
		return nil, fmt.Errorf("read row: %w", err)
	}

	// Decode row
	row := &Row{}
	_, err = h.r.f.fmt.DecodeFrom(bufio.NewReader(bytes.NewReader(encodedRow)), row)
	if err != nil {
		return nil, fmt.Errorf("decode row: %w", err)
	}

	return row.Value, nil
}

func (h *Versions) CurrentValue() ([]byte, error) { return h.Value(h.Length() - 1) }
