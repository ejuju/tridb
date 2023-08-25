package tridb

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

// File holds key-value pairs.
type File struct {
	mu      sync.RWMutex
	fpath   string
	keydir  *keydir
	r, w    *os.File
	woffset int
}

// Open opens the database file.
func Open(fpath string) (*File, error) {
	f := &File{fpath: fpath, keydir: &keydir{root: &keydirNode{}}}

	// Remove file possibly left from a crash during compaction.
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
		n, err := row.DecodeFrom(bufr)
		f.woffset += n
		if n == 0 && errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("decode row at offset %d: %w", f.woffset, err)
		}
		switch row.Op {
		default:
			return nil, fmt.Errorf("unknown row op %q at offset %d", row.Op, f.woffset)
		case OpSet:
			f.keydir.set(row.Key, &RowPosition{Offset: f.woffset - n, Size: n})
		case OpDelete:
			f.keydir.delete(row.Key)
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
	newKeymap := &keydir{root: &keydirNode{}}
	newOffset := 0
	newR, newW, err := openFileRW(f.fpath + CompactingFileExtension)
	if err != nil {
		return fmt.Errorf("open new datafile: %w", err)
	}

	// Write rows to new file
	err = f.keydir.walk(nil, func(key []byte, position *RowPosition) error {
		encodedRow := make([]byte, position.Size)
		_, err := f.r.ReadAt(encodedRow, int64(position.Offset))
		if err != nil {
			return fmt.Errorf("read current row: %w", err)
		}
		n, err := newW.Write(encodedRow)
		newOffset += n
		if err != nil {
			return fmt.Errorf("write new row: %w", err)
		}
		newKeymap.set(key, &RowPosition{Offset: newOffset - n, Size: n})
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk rows: %w", err)
	}

	// Sync new file
	err = newW.Sync()
	if err != nil {
		return fmt.Errorf("sync: %w", err)
	}

	// Replace old file with new
	err = os.Rename(newR.Name(), f.fpath)
	if err != nil {
		return fmt.Errorf("swap: %w", err)
	}
	f.keydir = newKeymap
	f.r, f.w = newR, newW
	f.woffset = newOffset
	_ = closeFileRW(f.r, f.w) // close old file (but ignore eventual error, unsignificant)
	return nil
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
