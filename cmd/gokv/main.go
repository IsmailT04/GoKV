package main

import (
	"fmt"
	"gokv"
	"os"
)

func main() {
	os.Remove("test.db")

	db, err := gokv.Open("test.db")
	if err != nil {
		panic(err)
	}
	defer db.Pager.Close()

	fmt.Printf("Initial Root Page ID: %d\n", db.Root)

	testCount := 1000000
	fmt.Println("Starting insertions...")
	for i := 0; i < testCount; i++ {
		key := []byte(fmt.Sprintf("user:%04d", i))
		val := []byte(fmt.Sprintf("value:%d", i))

		err := db.Put(key, val)
		if err != nil {
			panic(fmt.Sprintf("Insert failed at %d: %v", i, err))
		}

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
