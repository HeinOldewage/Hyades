package main

import (
	"github.com/HeinOldewage/Hyades"

	"database/sql"

	_ "github.com/mattn/go-sqlite3"
)

type DB struct {
	dbFile string
}

func NewDB(DBFile string) (*DB, error) {

	_, err := sql.Open("sqlite3", DBFile)
	if err != nil {
		return nil, err
	}

	return &DB{dbFile: DBFile}, nil
}

func (db *DB) GetNextJob() *Hyades.Work {

	return nil
}

func (db *DB) SaveWork(work *Hyades.Work) error {
	return nil
}

func init() {

}
