package tridb

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// File holds key-value pairs.
type File struct {
	mu        sync.RWMutex
	fpath     string
	newKeydir func() Keydir
	keydir    Keydir
	r, w      *os.File
	woffset   int
}

// Open opens the database file.
func Open(fpath string, newKeydir func() Keydir) (*File, error) {
	if newKeydir == nil {
		newKeydir = func() Keydir { return NewTrieKeydir() }
	}
	f := &File{fpath: fpath, keydir: newKeydir(), newKeydir: newKeydir}

	// Remove file possibly left over from a crash during last compaction.
	err := f.EnsureNoCompactingFile()
	if err != nil {
		return nil, fmt.Errorf("ensure no compacting file: %w", err)
	}

	// Open two file handlers (one in read-only, one in write-only)
	f.r, f.w, err = openFileRW(f.fpath)
	if err != nil {
		return nil, fmt.Errorf("open datafile: %w", err)
	}

	// Reconstruct in-memory state (= keydir: where keys/rows are located in the file)
	bufr := bufio.NewReader(f.r)
	row := &Row{}
	for {
		// Try to decode row
		n, err := row.DecodeFrom(bufr)
		f.woffset += n
		if n == 0 && errors.Is(err, io.EOF) {
			break // OK, we reached the end of the row (and it didn't happen in the middle of a row)
		}
		if err != nil {
			return nil, fmt.Errorf("decode row at offset %d: %w", f.woffset, err)
		}
		// Update keydir accordingly (and check for invalid op)
		switch row.Op {
		default:
			return nil, fmt.Errorf("unknown row op %q at offset %d", row.Op, f.woffset)
		case OpSet:
			f.keydir.Set(row.Key, &RowPosition{Offset: f.woffset - n, Size: n})
		case OpDelete:
			f.keydir.Delete(row.Key)
		}
	}

	return f, nil
}

// Close gracefully closes the underlying file handlers.
func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	return closeFileRW(f.r, f.w)
}

// File extension added to file during compaction process.
const CompactingFileExtension = ".compacting"

// Removes any remaining ".compacting" file left from an eventual past failed compaction.
// Does not fail if the file is not present.
func (f *File) EnsureNoCompactingFile() error {
	err := os.Remove(f.fpath + CompactingFileExtension)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

// Compact removes deleted keys and rewrites rows (in lexicographical order) to a new file.
func (f *File) Compact() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Remove any previous failed compaction file.
	err := f.EnsureNoCompactingFile()
	if err != nil {
		return fmt.Errorf("ensure no compacting file: %w", err)
	}

	// Init new file
	cleanKeydir := f.newKeydir()
	cleanOffset := 0
	cleanR, cleanW, err := openFileRW(f.fpath + CompactingFileExtension)
	if err != nil {
		return fmt.Errorf("open new datafile: %w", err)
	}

	// Write rows to new file
	err = f.keydir.Walk(nil, func(key []byte, position *RowPosition) error {
		encodedRow := make([]byte, position.Size)
		_, err := f.r.ReadAt(encodedRow, int64(position.Offset))
		if err != nil {
			return fmt.Errorf("read current row: %w", err)
		}
		n, err := cleanW.Write(encodedRow)
		cleanOffset += n
		if err != nil {
			return fmt.Errorf("write new row: %w", err)
		}
		cleanKeydir.Set(key, &RowPosition{Offset: cleanOffset - n, Size: n})
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk rows: %w", err)
	}

	// Sync new file
	err = cleanW.Sync()
	if err != nil {
		return fmt.Errorf("sync: %w", err)
	}

	// Close old file
	err = closeFileRW(f.r, f.w)
	if err != nil {
		return fmt.Errorf("close old file: %w", err)
	}

	// Replace old file with new
	err = os.Rename(cleanR.Name(), f.fpath)
	if err != nil {
		return fmt.Errorf("swap: %w", err)
	}
	f.keydir = cleanKeydir
	f.r, f.w = cleanR, cleanW
	f.woffset = cleanOffset
	return nil
}

// Copies the datafile to the given writer.
// Can be used to backup the datafile to another file or to a HTTP response writer for example.
func (f *File) CopyTo(dst io.Writer) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.r.Seek(0, io.SeekStart)
	n, err := io.Copy(dst, f.r)
	return int(n), err
}

// Path returns the path with which the database file was opened.
func (f *File) Path() string { return f.fpath }

func openFileRW(fpath string) (*os.File, *os.File, error) {
	r, err := os.OpenFile(fpath, os.O_RDONLY|os.O_CREATE, 0666)
	if err != nil {
		return nil, nil, err
	}
	w, err := os.OpenFile(fpath, os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		return nil, nil, err
	}
	return r, w, nil
}

func closeFileRW(r, w *os.File) error {
	rerr, werr := r.Close(), w.Close()
	if rerr != nil || werr != nil {
		return fmt.Errorf("close file (r/w): %w, %w", rerr, werr)
	}
	return nil
}

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
		return err // aborts on error
	}

	// Write rows to file
	startOffset := f.woffset
	for _, row := range w.rows {
		// Encode row
		encoded, err := row.Encode()
		if err != nil {
			err = fmt.Errorf("encode: %w", err)
			if f.woffset != startOffset {
				f.handleCorruption(err, startOffset)
			}
			return err
		}

		// Write row
		n, err := f.w.Write(encoded)
		f.woffset += n
		if err != nil {
			err = fmt.Errorf("write: %w", err)
			if f.woffset != startOffset {
				f.handleCorruption(err, startOffset)
			}
			return err
		}

		// Update memstate
		switch row.Op {
		default:
			panic("unreachable")
		case OpSet:
			f.keydir.Set(row.Key, &RowPosition{Offset: f.woffset - n, Size: n})
		case OpDelete:
			f.keydir.Delete(row.Key)
		}
	}

	// Sync file
	err = f.w.Sync()
	if err != nil {
		f.handleCorruption(fmt.Errorf("sync: %w", err), startOffset)
	}
	return nil
}

// Called when writing more than one row when data was already written to file and memstate updated,
// we need to recover from the file corruption and panic to restart the server
// with a clean memstate.
func (f *File) handleCorruption(err error, size int) {
	truncErr := os.Truncate(f.fpath, int64(size))
	if truncErr != nil {
		// Failed truncation, file is corrupted.
		panic(fmt.Errorf("file corruption (%d trailing bytes): %w: %w", f.woffset-size, err, truncErr))
	}
	// Truncation succeeded, avoided file corruption but memstate is corrupted, only need restart.
	panic(fmt.Errorf("memstate corruption: %w", err))
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

// Writer holds write operations executed in a write transaction.
type Writer struct{ rows []*Row }

// Set adds a new key-value pair to the database.
// Any previous key-value will be overwritten during the next compaction.
func (w *Writer) Set(key, value []byte) {
	w.rows = append(w.rows, &Row{Op: OpSet, Key: key, Value: value})
}

// Delete removes a key-value pair from the database.
//
// If the key does not exist, delete as no impact on the database state.
func (w *Writer) Delete(key []byte) {
	w.rows = append(w.rows, &Row{Op: OpDelete, Key: key})
}

// Reader can read rows from the database in a read transaction.
type Reader struct{ f *File }

// Has reports whether a key is known.
func (r *Reader) Has(key []byte) bool { return r.f.keydir.Get(key) != nil }

// Get returns the eventual value associated with the given key,
// if the key is not found, a nil value is returned.
// If an error is returned, it is internal (failed OS read or decoding).
func (r *Reader) Get(key []byte) ([]byte, error) {
	position := r.f.keydir.Get(key)
	if position == nil {
		return nil, nil
	}
	row, err := r.readAndDecode(position)
	if err != nil {
		return nil, err
	}
	return row.Value, nil
}

// Count returns the number of unique keys in the database.
func (r *Reader) Count() int { return r.f.keydir.Count() }

// Count prefix returns the number of unique keys with the given prefix.
func (r *Reader) CountPrefix(prefix []byte) int {
	count := 0
	_ = r.f.keydir.Walk(&WalkOptions{Prefix: prefix}, func(key []byte, position *RowPosition) error {
		count++
		return nil
	})
	return count
}

// Walk iterates over the keys in the database.
// It stops iterating if an error is returned in the callback.
//
// Note: The returned error can only originate from the callback.
func (r *Reader) Walk(opts *WalkOptions, do func(key []byte) error) error {
	return r.f.keydir.Walk(opts, func(key []byte, _ *RowPosition) error { return do(key) })
}

// WalkWithValue iterates over the key-value pairs in the database.
// It stops iterating if an error is returned in the callback.
// If you only need to access keys but not the value, use Walk instead.
//
// Basically like Walk but the callback also gets the value associated with the key.
func (r *Reader) WalkWithValue(opts *WalkOptions, do func(key, value []byte) error) error {
	return r.f.keydir.Walk(opts, func(key []byte, position *RowPosition) error {
		row, err := r.readAndDecode(position)
		if err != nil {
			return err
		}
		return do(key, row.Value)
	})
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

// ErrBreak is an error that can be used to abort a read-write transaction or exit a walk.
// It is only declared for convenience, it does nothing special.
var ErrBreak = errors.New("break")
