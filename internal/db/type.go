package db

import (
	"context"
	"time"

	"github.com/jmoiron/sqlx"
)

// https://www.sqlite.org/pragma.html#pragma_journal_mode
const (
	DELETE = iota
	TRUNCATE
	PERSIST
	MEMORY
	WAL
	OFF
)

// https://www.sqlite.org/sharedcache.html
const (
	SHARED = iota
	PRIVATE
)

// Sqlite is the externed sqlite type
type Sqlite struct {
	ctx     context.Context
	name    string
	uuid    string
	journal int
	cache   int
	rotate  int
	db      *sqlx.DB
}

type DB interface {
	Close()
	Insert(msg string) error
	Start(startTime time.Time) error
	Stop(endTime time.Time) error
}
