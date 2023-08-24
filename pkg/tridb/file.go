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

// Row holds data about a database operation,
// more specifically, a 'set' or 'delete' operation.
// Rows are persisted to a file.
type Row struct {
	Op         byte
	Key, Value []byte
}

// Write operations encoded into rows.
const (
	OpSet    byte = '+'
	OpDelete byte = '-'
)

// File holds key-value pairs.
type File struct {
	mu      sync.RWMutex
	fpath   string
	fmt     Format
	km      *keymap
	r, w    *os.File
	woffset int
}

func Open(fpath string, format Format) (*File, error) {
	if format == nil {
		format = DefaultFormat
	}
	f := &File{fpath: fpath, fmt: format, km: &keymap{root: &keymapNode{}}}

	// Remove remaining ".compacting" file from eventual failed compaction
	// If not removed, could clash with compaction if it was already partially written.
	err := os.Remove(f.fpath + CompactingFileExtension)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("remove remaining compacting file: %w", err)
	}

	// Open file
	f.r, f.w, err = openFileRW(f.fpath)
	if err != nil {
		return nil, fmt.Errorf("open datafile: %w", err)
	}

	// Reconstruct in-memory state (where keys are located in the file)
	bufr := bufio.NewReader(f.r)
	row := &Row{}
	for {
		n, err := f.fmt.DecodeFrom(bufr, row)
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
			f.km.set(row.Key, [2]int{f.woffset - n, n})
		case OpDelete:
			f.km.delete(row.Key)
		}
	}

	return f, nil
}

func (f *File) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()

	return closeFileRW(f.r, f.w)
}

const CompactingFileExtension = ".compacting"

func (f *File) Compact(newFormat Format) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if newFormat == nil {
		newFormat = f.fmt
	}

	// Init new file
	newKeymap := &keymap{root: &keymapNode{}}
	newOffset := 0
	newR, newW, err := openFileRW(f.fpath + CompactingFileExtension)
	if err != nil {
		return fmt.Errorf("open new datafile: %w", err)
	}

	// Write rows to new file
	err = f.km.walk(nil, func(key []byte, history [][2]int) error {
		for _, position := range history {
			// Decode current row
			encodedRow := make([]byte, position[1])
			_, err := f.r.ReadAt(encodedRow, int64(position[0]))
			if err != nil {
				return fmt.Errorf("read current row: %w", err)
			}
			row := &Row{}
			_, err = f.fmt.DecodeFrom(bufio.NewReader(bytes.NewReader(encodedRow)), row)
			if err != nil {
				return fmt.Errorf("decode current row: %w", err)
			}

			// Write new row and update new memstate
			newEncodedRow, err := newFormat.Encode(row)
			if err != nil {
				return fmt.Errorf("encode new row: %w", err)
			}
			n, err := newW.Write(newEncodedRow)
			newOffset += n
			if err != nil {
				return fmt.Errorf("write new row: %w", err)
			}
			newKeymap.set(key, [2]int{newOffset - n, n})
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk rows: %w", err)
	}

	// Swap old and new file
	err = os.Rename(newR.Name(), f.fpath)
	if err != nil {
		return fmt.Errorf("swap: %w", err)
	}
	f.fmt = newFormat
	f.km = newKeymap
	f.r, f.w = newR, newW
	f.woffset = newOffset
	return nil
}

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

type keymap struct {
	root *keymapNode
}

type keymapNode struct {
	children [256]*keymapNode
	history  [][2]int
}

func (km *keymap) set(key []byte, position [2]int) {
	curr := km.root
	for _, c := range key {
		if curr.children[c] == nil {
			curr.children[c] = &keymapNode{}
		}
		curr = curr.children[c]
	}
	curr.history = append(curr.history, position)
}

func (km *keymap) delete(key []byte) {
	curr := km.root
	for _, c := range key {
		if curr.children[c] == nil {
			return // no-op if key not found
		}
		curr = curr.children[c]
	}
	curr.history = nil
}

func (km *keymap) get(key []byte) [][2]int {
	curr := km.root
	for _, c := range key {
		if curr.children[c] == nil {
			return nil
		}
		curr = curr.children[c]
	}
	return curr.history
}

type WalkOptions struct {
	Prefix  []byte // only walk over keys with the given prefix
	Reverse bool   // reverse lexicographical order
}

func (km *keymap) walk(opts *WalkOptions, do func(key []byte, history [][2]int) error) error {
	if opts == nil {
		opts = &WalkOptions{Prefix: []byte{}}
	} else if opts.Prefix == nil {
		opts.Prefix = []byte{}
	}
	curr := km.root
	for _, c := range opts.Prefix {
		if curr.children[c] == nil {
			return nil
		}
		curr = curr.children[c]
	}
	return curr.walk(opts.Reverse, opts.Prefix, do)
}

func (n *keymapNode) walk(reverse bool, prefix []byte, do func(key []byte, history [][2]int) error) error {
	if n.history != nil {
		err := do(prefix, n.history)
		if err != nil {
			return err
		}
	}

	// Walk in reverse lexicographical order
	if reverse {
		for c := len(n.children) - 1; c >= 0; c-- {
			child := n.children[c]
			if child == nil {
				continue
			}
			err := child.walk(reverse, append(prefix, byte(c)), do)
			if err != nil {
				return err
			}
		}
		return nil
	}

	// Walk in lexicographical order
	for c := 0; c < len(n.children); c++ {
		child := n.children[c]
		if child == nil {
			continue
		}
		err := child.walk(reverse, append(prefix, byte(c)), do)
		if err != nil {
			return err
		}
	}
	return nil
}
