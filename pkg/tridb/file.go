package tridb

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/ejuju/tridb/pkg/fidx"
)

// File holds key-value pairs.
type File struct {
	mu         sync.RWMutex
	fpath      string
	numBuckets int
	idx        *fidx.LHTIndex
	r, w       *os.File
	woffset    int
}

// Open opens the database file.
func Open(fpath string, numBuckets int) (*File, error) {
	if numBuckets <= 0 {
		numBuckets = 1
	}
	f := &File{fpath: fpath, idx: fidx.NewLHTIndex(numBuckets), numBuckets: numBuckets}

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
	row := Row{}
	for {
		n, err := row.DecodeFrom(bufr)
		f.woffset += n
		if n == 0 && errors.Is(err, io.EOF) {
			break // OK, we reached the end of the row (and it didn't happen in the middle of a row)
		}
		if err != nil {
			return nil, fmt.Errorf("decode row at offset %d: %w", f.woffset, err)
		}
		if row.IsDeleted {
			f.idx.Delete(row.Key)
		} else {
			f.idx.Put(row.Key, fidx.Position{f.woffset - n, n})
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

var (
	ErrMemoryCorruption = errors.New("memory corruption")
	ErrFileCorruption   = errors.New("file corruption")
)

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
	cleanIdx := fidx.NewLHTIndex(f.numBuckets)
	cleanOffset := 0
	cleanR, cleanW, err := openFileRW(f.fpath + CompactingFileExtension)
	if err != nil {
		return fmt.Errorf("open new datafile: %w", err)
	}

	// Write rows to new file
	for row := f.idx.Oldest; row != nil; row = row.Next {
		encodedRow := make([]byte, row.Position.Size())
		_, err := f.r.ReadAt(encodedRow, int64(row.Position.Offset()))
		if err != nil {
			return fmt.Errorf("read row: %w", err)
		}
		n, err := cleanW.Write(encodedRow)
		cleanOffset += n
		if err != nil {
			return fmt.Errorf("write to new file: %w", err)
		}
		cleanIdx.Put(row.Key, fidx.Position{cleanOffset - n, n})
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
	f.idx = cleanIdx
	f.r, f.w = cleanR, cleanW
	f.woffset = cleanOffset
	return nil
}

func (f *File) readAndDecodeRow(position fidx.Position) (*Row, error) {
	encodedRow := make([]byte, position.Size())
	_, err := f.r.ReadAt(encodedRow, int64(position.Offset()))
	if err != nil {
		return nil, fmt.Errorf("read row: %w", err)
	}
	row := &Row{}
	_, err = row.DecodeFrom(bytes.NewReader(encodedRow))
	if err != nil {
		return nil, fmt.Errorf("decode row: %w", err)
	}
	return row, nil
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
		if row.IsDeleted {
			f.idx.Delete(row.Key)
		} else {
			f.idx.Put(row.Key, fidx.Position{f.woffset - n, n})
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
		panic(fmt.Errorf("%w (%d): %w: %w", ErrFileCorruption, f.woffset-size, err, truncErr))
	}
	// Truncation succeeded, avoided file corruption but memstate is corrupted, only need restart.
	panic(fmt.Errorf("%w: %w", ErrMemoryCorruption, err))
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
type Writer struct {
	rows []*Row
}

// Set adds a new key-value pair to the database.
// Any previous key-value will be overwritten during the next compaction.
func (w *Writer) Set(key, value []byte) {
	w.rows = append(w.rows, &Row{Key: key, Value: value})
}

// Delete removes a key-value pair from the database.
//
// If the key does not exist, delete as no impact on the database state.
func (w *Writer) Delete(key []byte) {
	w.rows = append(w.rows, &Row{IsDeleted: true, Key: key})
}

// Reader can read rows from the database in a read transaction.
type Reader struct {
	f *File
}

// Has reports whether a key is known.
func (r *Reader) Has(key []byte) bool { return r.f.idx.Get(key) != nil }

// Count returns the number of unique keys in the database.
func (r *Reader) Count() int { return r.f.idx.Count }

// Get returns the eventual value associated with the given key,
// if the key is not found, a nil value is returned.
// If an error is returned, it is internal (failed OS read or decoding).
func (r *Reader) Get(key []byte) ([]byte, error) {
	rowInfo := r.f.idx.Get(key)
	if rowInfo == nil {
		return nil, nil
	}
	row, err := r.f.readAndDecodeRow(rowInfo.Position)
	if err != nil {
		return nil, err
	}
	return row.Value, nil
}

type RowReader struct {
	r       *Reader
	current *fidx.RowInfo
}

func (r *Reader) Oldest() *RowReader {
	oldest := r.f.idx.Oldest
	if oldest == nil {
		return nil
	}
	return &RowReader{r: r, current: oldest}
}

func (r *Reader) Latest() *RowReader {
	latest := r.f.idx.Latest
	if latest == nil {
		return nil
	}
	return &RowReader{r: r, current: latest}
}

func (r *Reader) Seek(key []byte) *RowReader {
	rinfo := r.f.idx.Get(key)
	if rinfo == nil {
		return nil
	}
	return &RowReader{r: r, current: rinfo}
}

func (c *RowReader) Key() []byte { return c.current.Key }

func (c *RowReader) Value() ([]byte, error) {
	row, err := c.r.f.readAndDecodeRow(c.current.Position)
	if err != nil {
		return nil, err
	}
	return row.Value, nil
}

func (c *RowReader) Previous() *RowReader {
	if c.current.Previous == nil {
		return nil
	}
	return &RowReader{r: c.r, current: c.current.Previous}
}

func (c *RowReader) Next() *RowReader {
	if c.current.Next == nil {
		return nil
	}
	return &RowReader{r: c.r, current: c.current.Next}
}
