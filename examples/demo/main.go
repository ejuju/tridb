package main

import (
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
		num := r.Count() // Count all unique keys
		log.Println(num)
		return nil
	})
	if err != nil {
		panic(err)
	}

	// Iterate over keys
	_ = f.Read(func(r *tridb.Reader) error {
		c := r.Cursor()
		for key := c.Last(); key != nil; c.Previous() {
			log.Println(key)
		}
		return nil
	})
}

func init() {
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
}
