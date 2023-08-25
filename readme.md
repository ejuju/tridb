# TriDB: Tiny persistent key-value store for your tiny Go project

Features:
- [x] ACID-compliant
- [x] Embedded (just a library, your database is embedded in your executable)
- [x] Database files are plain-text encoded, they can be shown and modified in a text-editor.
- [x] Built for prototypes and small projects
- [x] Zero-dependecy tiny codebase (less than 1000 SLOC)
- [x] Simple log and index design (does not use a B+Tree or LSM, inspired by Riak's Bitcask)

Quirks, limitations and potential gotchas:
- Keys are stored in memory (in a trie)
- Max key length is 255
- Max value length is around 4.2 GB
- No sub-groups of key-value pairs, everything is in the same "bucket" / "table" / "collection".
	You may implement this by using a pre-defined key-suffix for each collection.
	or using a dedicated file for each collection.
- Lacks reliable file corruption recovery (ex: failed disk I/O write operations)
- When reading a key-value pair, the returned value will be nil when the key is not found (and no error is returned).
	Callers should check for a nil value when the key may not exist. (deliberate design decision)
- Limited to a single process (as embedded databases go)

References:
- https://scholar.harvard.edu/files/stratos/files/keyvaluestorageengines.pdf
- https://riak.com/assets/bitcask-intro.pdf