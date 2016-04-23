package main

import (
	"database/sql"
	"fmt"
	"os"
	// _ "github.com/mattn/go-sqlite3"
)

func openDatabase(path string) (*sql.DB, error) {
	commands := [2]string{
		"pragma synchronous = off;",
		"pragma journal_mode = off;"}

	db, err := sql.Open("sqlite3", path)

	for _, command := range commands {
		_, err = db.Exec(command)
		if err != nil {
			fmt.Printf("%q: %s\n", err, command)
			db.Close()
		}
	}
	return db, err // nil *sql.DB = non-nil error, and vice versa
}

func createDatabase(path string) (*sql.DB, error) {
	commands := [3]string{
		"pragma synchronous = off;",
		"pragma journal_mode = off;",
		"create table pairs (key text, value text);"}

	os.Remove(path) // remove db file if it already exists

	db, err := sql.Open("sqlite3", path)
	defer db.Close()
	if err != nil {
		if os.IsNotExist(err) {
			fmt.Println("FILE DOES NOT EXIST")
		} else {
			fmt.Println(err)
		}
	} else {
		for _, command := range commands {
			_, err = db.Exec(command)
			if err != nil {
				fmt.Printf("%q: %s\n", err, command)
			}
		}
	}
	return db, err
}

// paths, err := splitDatabase("input.sqlite3", "output-%d.sqlite3", 50)
func splitDatabase(source, outputPattern string, m int) ([]string, error) {
	db, err := openDatabase(source)
	if err != nil {
		fmt.Printf("FAILED TO OPEN DATABASE %s", source)
	}
	count, err := db.Query("select count(1) from pairs")

	if count < m {
		err = "ERROR!"
		return nil, err
	}

	for i := 0; i < m; i++ {

	}
	return nil, nil
}
