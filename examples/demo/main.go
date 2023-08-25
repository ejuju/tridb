package main

import (
	"errors"
	"log"

	"github.com/ejuju/tridb/pkg/tridb"
)

func main() {
	// Open the database file
	f, err := tridb.Open("main.tridb")
	if err != nil {
		panic(err)
	}
	defer f.Close()

	// Compact the database file
	//
	// Compacting the file consists in removing deleted key-value pairs
	// and re-writing the key-value pairs ordered lexicographically by key.
	// Key-value pairs with the same key are kept in the order they were written in (chronological).
	err = f.Compact()
	if err != nil {
		panic(err)
	}

	// Set a key-value pair
	err = f.ReadWrite(func(r *tridb.Reader, w *tridb.Writer) error {
		w.Set([]byte("my-key"), []byte("my-value"))
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Delete a key-value pair
	err = f.ReadWrite(func(r *tridb.Reader, w *tridb.Writer) error {
		w.Delete([]byte("my-key2"))
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Read a key-value pair
	//
	// Note: Value is nil if key not found.
	_ = f.Read(func(r *tridb.Reader) error {
		v, err := r.Get([]byte("my-key"))
		log.Println(v, err)
		return nil
	})

	// Check if a key exists
	_ = f.Read(func(r *tridb.Reader) error {
		if r.Has([]byte("my-key")) {
			log.Println("found key")
		}
		return nil
	})

	// Count the number of unique keys
	_ = f.Read(func(r *tridb.Reader) error {
		num := r.Count([]byte{})                // Count all unique keys
		numWithPrefix := r.Count([]byte("my-")) // Count unique keys with prefix
		log.Println(num, numWithPrefix)
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Iterate over keys
	//
	// Note: return an error if you want to stop iterating early.
	_ = f.Read(func(r *tridb.Reader) error {
		// Iterate over all unique keys (in lexicographical order)
		_ = r.Walk(nil, func(key []byte) error {
			log.Println(key)
			return nil
		})

		// Iterate over all unique keys (in reverse lexicographical order)
		_ = r.Walk(&tridb.WalkOptions{Reverse: true}, func(key []byte) error {
			log.Println(key)
			return nil
		})

		// Iterate over keys with a given prefix
		_ = r.Walk(&tridb.WalkOptions{Prefix: []byte("my-")}, func(key []byte) error {
			log.Println(key)
			return nil
		})

		// Iterate over all keys and (current) values (in lexicographical order)
		_ = r.WalkWithValue(nil, func(key, value []byte) error {
			log.Println(key, value)
			return nil
		})

		return nil
	})

	// Pagination
	items := []string{}
	err = f.Read(func(r *tridb.Reader) error {
		page := 42         // arbitrary user defined input
		itemsPerPage := 10 // arbitrary user defined input
		offset := page * itemsPerPage
		i := -1
		return r.Walk(nil, func(key []byte) error {
			i++
			if i > offset+itemsPerPage {
				return tridb.ErrBreak
			} else if i < offset {
				return nil
			}
			items = append(items, string(key))
			return nil
		})
	})
	if err != nil && !errors.Is(err, tridb.ErrBreak) {
		panic(err)
	}
}

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}
