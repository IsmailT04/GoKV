package main

import (
	"fmt"
	"gokv"
	"os"
)

func main() {
	// 1. Clean up old test data
	os.Remove("test.db")

	// 2. Open DB
	db, err := gokv.Open("test.db")
	if err != nil {
		panic(err)
	}
	defer db.Pager.Close()

	fmt.Printf("Initial Root Page ID: %d\n", db.Root)

	// 3. Insert Data
	// A Leaf holds ~4KB.
	// Key="user:X" (6 bytes), Val="value:X" (7 bytes).
	// Overhead = 6 (header) + 2 (offset) = 8. Total per entry ~21 bytes.
	// 4096 / 21 ≈ 190 entries per page.
	// Inserting 250 entries guarantees a split!

	testCount := 1000000
	fmt.Println("Starting insertions...")
	for i := 0; i < testCount; i++ {
		key := []byte(fmt.Sprintf("user:%04d", i)) // Pad with zeros for sorting: user:0001
		val := []byte(fmt.Sprintf("value:%d", i))

		err := db.Put(key, val)
		if err != nil {
			panic(fmt.Sprintf("Insert failed at %d: %v", i, err))
		}

		// Monitor the Root ID
		if i%1000 == 0 {
			fmt.Printf("Inserted %d keys. Root Page ID is now: %d\n", i, db.Root)
		}
	}

	fmt.Printf("Final Root Page ID: %d\n", db.Root)
	if db.Root == 0 {
		fmt.Println("WARNING: Root did not change. Did we actually split?")
	} else {
		fmt.Println("SUCCESS: Root split detected! Tree height increased.")
	}

	// 4. Verify Data (Read Back)
	fmt.Println("Verifying data...")
	for i := 0; i < testCount; i++ {
		key := []byte(fmt.Sprintf("user:%04d", i))
		expectedVal := []byte(fmt.Sprintf("value:%d", i))

		val, err := db.Get(key)
		if err != nil {
			panic(fmt.Sprintf("Read failed for %s: %v", key, err))
		}

		if string(val) != string(expectedVal) {
			panic(fmt.Sprintf("Data mismatch! Key: %s, Expected: %s, Got: %s", key, expectedVal, val))
		}
	}

	fmt.Println("All 250 keys verified successfully!")
}
