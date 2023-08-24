# TriDB: Tiny persistent key-value store for your tiny Go project

Features:
- [x] ACID-compliant
- [x] Embedded (just a library, your database is embedded in your executable)
- [x] Database files are plain-text encoded, they can be shown and edited in any text-editor.
- [x] A key can have multiple values (like a history)
- [x] Built for prototypes and small projects

Quirks, limitations and potential gotchas:
- Keys are stored in memory (in a trie)
- No sub-groups of key-value pairs, everything is in the same "bucket" / "table" / "collection"
- Lacks reliable file corruption recovery (for failed disk I/O write operations)
- When reading a key-value pair, the returned value will be nil when the key is not found (and no error is returned).
	Callers should instead check for a nil value when the key may not exist.
- Limited to a single process (as embedded databases go)

## Use as Go library (embedded database)

```shell
# Add the module to your dependencies
go get github.com/ejuju/tridb
```

```go
// Open the database file
f, err := tridb.Open("main.tridb", nil)
if err != nil {
	panic(err)
}
defer f.Close()

// Compact the database file
//
// Compacting the file consists in removing deleted key-value pairs 
// and re-writing the key-value pairs ordered lexicographically by key.
// Key-value pairs with the same key are kept in the order they were written in (chronological).
// During compaction, the file encoding format may be changed to a new one.
err = f.Compact(nil)
if err != nil {
	panic(err)
}

// Add a key-value pair
//
// Note: To abort the transaction, the callback should return a non-nil error.
// Callback errors are returned back to the caller.
//
// When adding a key-value pair, eventual previous key-value pairs with the same key are not erased.
// Instead they are simply kept in the file and in memory. Previous versions can then be accessed.
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
//
// Note: the returned error can only originate from the callback, therefore it can be ignored if the
// callback never fails (for example, when using `r.Has`, `r.Walk` or `r.Count`).
_ = f.Read(func(r *tridb.Reader) error {
	v, err := r.Get([]byte("my-key")).CurrentValue()
	log.Println(v, err)
	return nil
})

// Read past versions of a key-value pair
_ = f.Read(func(r *tridb.Reader) error {
	versions := r.Get([]byte("my-key"))

	l := versions.Length()      // Get the number of versions (0 if key not found)
	v, err := versions.Value(0) // Get the oldest version
	log.Println(l, v, err)

	// Iterate over all past versions (from oldest to newest)
	for i := 0; i < versions.Length(); i++ {
		v, err := versions.Value(i)
		log.Println(v, err)
	}

	// Iterate over all past versions (from newest to oldest)
	for i := versions.Length() - 1; i >= 0; i-- {
		v, err := versions.Value(i)
		log.Println(v, err)
	}
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

	// Iterate over all keys and (current) values (in lexicographical order)
	_ = r.WalkWithVersions(nil, func(key []byte, versions *tridb.Versions) error {
		v, err := versions.CurrentValue()
		log.Println(key, v, err)
		return nil
	})

	// Iterate over keys (in reverse lexicographical order)
	_ = r.Walk(&tridb.WalkOptions{Reverse: true}, func(key []byte) error {
		log.Println(key)
		return nil
	})

	// Iterate over keys with a given prefix
	_ = r.Walk(&tridb.WalkOptions{Prefix: []byte("my-")}, func(key []byte) error {
		log.Println(key)
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
```

## Use as CLI REPL

```shell
# Install executable
go install github.com/ejuju/tridb 

# Run REPL in CLI (press enter to show the available commands)
# ex: tridb main.tridb auto
tridb $FILE_PATH $ENCODING_FORMAT
```
