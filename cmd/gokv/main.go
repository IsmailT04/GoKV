package main

import (
	"bufio"
	"fmt"
	"gokv"
	"os"
	"strings"
)

func main() {
	db, err := gokv.Open("my.db")
	if err != nil {
		panic(err)
	}
	defer db.Pager.Close()

	fmt.Println("Welcome to GoKV! Type 'help' for commands.")
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("gokv> ")
		if !scanner.Scan() {
			break
		}
		line := scanner.Text()
		parts := strings.Fields(line)
		if len(parts) == 0 {
			continue
		}

		cmd := parts[0]

		switch cmd {
		case "put":
			if len(parts) != 3 {
				fmt.Println("Usage: put <key> <value>")
				continue
			}
			err := db.Update(func(tx *gokv.Tx) error {
				return tx.Put([]byte(parts[1]), []byte(parts[2]))
			})
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			} else {
				fmt.Println("OK")
			}

		case "get":
			if len(parts) != 2 {
				fmt.Println("Usage: get <key>")
				continue
			}
			err := db.View(func(tx *gokv.Tx) error {
				val, err := tx.Get([]byte(parts[1]))
				if err != nil {
					return err
				}
				fmt.Printf("Value: %s\n", string(val))
				return nil
			})
			if err != nil {
				fmt.Printf("Error: %v\n", err)
			}

		case "exit", "quit":
			return

		case "help":
			fmt.Println("Commands: put <k> <v>, get <k>, exit")

		default:
			fmt.Println("Unknown command")
		}
	}
}
