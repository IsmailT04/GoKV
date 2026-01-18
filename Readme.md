# GoKV: A Persistent B+ Tree Database

**GoKV** is a crash-safe, ACID-compliant embedded key-value store written from scratch in Go.

It implements a disk-based **B+ Tree** storage engine using **Copy-On-Write (COW)** semantics, similar to [BoltDB](https://github.com/boltdb/bolt) and [LMDB](http://www.lmdb.tech/doc/). This project was built to understand the low-level mechanics of database systems, including page management, serialization, and transaction isolation.

## Features

* **B+ Tree Indexing:** Efficient  lookups and range scans.
* **ACID Transactions:** Full support for atomic **Read-Write** (`Update`) and **Read-Only** (`View`) transactions.
* **Crash Safety:** Uses Copy-On-Write (COW) to ensure the database file is never corrupted, even during power failure.
* **Concurrency Control:** Thread-safe with `sync.RWMutex` allowing multiple concurrent readers and a single writer.
* **Paged Storage:** Abstracts the filesystem into fixed-size 4KB blocks (Pages).

## Installation

```bash
git clone https://github.com/IsmailT04/GoKV.git
cd gokv
go run ./cmd/gokv

```

## Usage API

GoKV provides a simple, idiomatic Go API for embedding into applications.

```go
package main

import (
	"fmt"
	"log"
	"gokv"
)

func main() {
	// 1. Open the database
	db, err := gokv.Open("my.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Pager.Close()

	// 2. Write Data (Atomic Transaction)
	err = db.Update(func(tx *gokv.Tx) error {
		return tx.Put([]byte("user:101"), []byte("Ismail"))
	})
	if err != nil {
		log.Fatal(err)
	}

	// 3. Read Data (Concurrent Safe)
	err = db.View(func(tx *gokv.Tx) error {
		val, err := tx.Get([]byte("user:101"))
		if err != nil {
			return err
		}
		fmt.Printf("User: %s\n", string(val))
		return nil
	})
}

```

## CLI Tool

The project includes an interactive CLI to inspect and manipulate the database file manually.

```bash
$ go run ./cmd/gokv
Welcome to GoKV! Type 'help' for commands.
gokv> put user1 ismail
OK
gokv> get user1
Value: ismail
gokv> exit

```

## Architecture & Internals

### 1. The Pager (Physical Layer)

The database file is treated as a linear array of **4KB Pages**.

* **Page 0 (Meta):** The "Superblock" containing the pointer to the current Root of the tree.
* **Page 1..N:** Data pages containing B+ Tree nodes.

### 2. The B+ Tree (Logical Layer)

Data is stored in a balanced tree structure.

* **Leaf Nodes:** Store actual Key/Value pairs.
* **Branch Nodes:** Store internal navigation pointers (Child Page IDs).
* **Split Algorithm:** When a node fills up (4KB), it splits into two, promoting the median key to the parent. This increases tree height dynamically.

### 3. Transaction Management (ACID)

GoKV uses **Copy-On-Write** for updates:

1. **Begin:** A transaction creates an in-memory "dirty map".
2. **Write:** Modified nodes are *not* written over the old data. Instead, they are copied, modified, and assigned a *new* page ID.
3. **Commit:**
* The dirty pages are flushed to disk (new locations).
* The **Meta Page** is updated atomically to point to the new Root.
* *Result:* If a crash happens before the Meta update, the DB effectively "rolls back" to the state before the transaction.



## Future Improvements

* **Freelist Persistence:** Currently, freed pages are tracked in memory. Persisting a free list to disk would allow reusing space across restarts.
* **Delete Operation:** Implementing key deletion and node merging (rebalancing).
* **Range Scans:** Adding cursors (`Seek`, `Next`) for iterating over keys.

## References

* **BoltDB:** The primary architectural inspiration.
* **SQLite:** For the concept of the Pager and B-Tree integration.
* **"Database Internals" by Alex Petrov:** A great resource for storage engine theory.
