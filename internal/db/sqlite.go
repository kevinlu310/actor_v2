// +build database

package db

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"time"

	l "github.com/vsdmars/actor/internal/logger"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"go.uber.org/zap"
)

// It is easy to get into trouble by accidentally holding on to connections.
// To prevent this:
// Ensure you Scan() every Row object
// Ensure you either Close() or fully-iterate via Next() every Rows object
// Ensure every transaction returns its connection via Commit() or Rollback()
// Note that Rows.Close() can be called multiple times safely,
// so do not fear calling it where it might not be necessary.

const (
	backupDB = iota
	rotateDB
)

const (
	// create directory for sqlite db file due to sqlite sync by directory
	backupDir = "sqlitedb"
	rotateDir = "sqlitedb_rotate"
	dbDSN     = "file:%s?cache=%s&_journal=%s&_sync=OFF"
)

var journalMode = map[int]string{
	DELETE:   "delete",
	TRUNCATE: "truncate",
	PERSIST:  "persist",
	MEMORY:   "memory",
	WAL:      "wal",
	OFF:      "off",
}

var cacheMode = map[int]string{
	SHARED:  "shared",
	PRIVATE: "private",
}

var (
	insertActorStart = `INSERT INTO actor(uuid, name, start_time) VALUES (:uuid, :name, :start_time) ;`
	updateActorEnd   = `UPDATE actor SET end_time = :end_time WHERE uuid = :uuid ;`
	insertLog        = `INSERT INTO log(time, message) VALUES (:time, :message) ;`
)

var (
	ErrDbPathIsAFile = "%s is an existing file"
	ErrDbClosed      = errors.New("DB is closed")
)

var actor_schema = `
CREATE TABLE if not exists actor(
    uuid text PRIMARY KEY,
    name text,
    start_time text,
    end_time text
);
`
var log_schema = `
CREATE TABLE if not exists log(
    seq INTEGER PRIMARY KEY ASC,
    time text,
    message text
);
`

// database ORM types
type (
	message struct {
		Msg string `json:"message"`
	}

	actor struct {
		Uuid  string `db:"uuid"`
		Name  string `db:"name"`
		Stime string `db:"start_time"`
		Etime string `db:"end_time"`
	}

	log struct {
		Seq  int    `db:"seq"`
		Time string `db:"time"`
		Msg  []byte `db:"message"`
	}
)

// NewSqlite returns init. Sqlite instance
//
// ctx: context.Context
//
// name: actor name
//
// uuid: actor uuid
//
// jmode: journal mode
//
// cmode: cache mode
//
// rcnt: 0: no rotation, >0: preserve number of records then rotate.
func NewSqlite(
	ctx context.Context, // caller's context
	name string, // actor name
	uuid string, // actor uuid
	jmode int, // journal mode
	cmode int, // cache mode
	rcnt int, // rotate count
	period int, // recycle period in seconds
) (*Sqlite, error) {

	db, err := initDB(ctx, name, uuid, jmode, cmode, backupDB)
	if err != nil {
		l.Logger.Error(
			"backup initDB error",
			zap.String("service", serviceName),
			zap.String("actor", name),
			zap.String("uuid", uuid),
			zap.String("error", err.Error()),
		)
		return nil, err
	}

	if rcnt > 0 {
		go rotate(ctx, name, uuid, db, rcnt, period)
	}

	return &Sqlite{
		ctx:     ctx,
		name:    name,
		uuid:    uuid,
		journal: jmode,
		cache:   cmode,
		rotate:  rcnt,
		db:      db,
	}, nil
}

func initDB(
	ctx context.Context,
	name, uuid string,
	jmode, cmode int,
	dbType int) (*sqlx.DB, error) {

	var dbPath string
	currentDir, _ := os.Getwd()

	switch dbType {
	case backupDB:
		dbPath = path.Join(currentDir, backupDir)
	case rotateDB:
		dbPath = path.Join(currentDir, rotateDir)
	default:
		dbPath = path.Join(currentDir, backupDir)
	}

	if fi, err := os.Stat(dbPath); err != nil {
		if err := os.Mkdir(dbPath, 0700); err != nil {
			return nil, err
		}
	} else if !fi.IsDir() {
		// dbPath is a file, not directory
		return nil, fmt.Errorf(ErrDbPathIsAFile, dbPath)
	}

	var dbFile string
	gpattern := `%s_%s_*.db`
	rpattern := `%s_%s_(?P<SEQ>\d+).db`

	switch dbType {
	case backupDB:
		dbFile = path.Join(dbPath, fmt.Sprintf("%s_%s.db", name, uuid))
	case rotateDB:
		dbFiles := path.Join(dbPath, fmt.Sprintf(gpattern, name, uuid))

		if m, err := filepath.Glob(dbFiles); err != nil {
			dbFile = path.Join(dbPath, fmt.Sprintf("%s_%s_1.db", name, uuid))
		} else {
			re, err := regexp.Compile(fmt.Sprintf(rpattern, name, uuid))
			if err != nil {
				return nil, err
			}

			maxCnt := 0

			for idx := range m {
				match := re.FindStringSubmatch(m[idx])

				for i, name := range re.SubexpNames() {
					if i != 0 && name == "SEQ" {
						v, _ := strconv.Atoi(match[i])
						maxCnt = max(v, maxCnt)
					}
				}
			}

			dbFile = path.Join(
				dbPath, fmt.Sprintf("%s_%s_%d.db", name, uuid, maxCnt+1))
		}
	default:
		dbFile = path.Join(dbPath, fmt.Sprintf("%s_%s.db", name, uuid))
	}

	dsn := fmt.Sprintf(dbDSN, dbFile, cacheMode[cmode], journalMode[jmode])

	// Use open instead of MustOpen, which panics if can't open
	// Use open instead of Connect since sqlite is local file
	db, err := sqlx.Open(
		"sqlite3",
		dsn,
	)
	if err != nil {
		return nil, err
	}

	// The connection is returned to the pool before every call's result
	// is returned.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	// sqlite Exec can't handle multi-statement, thus makes it different stmnt
	db.MustExecContext(ctx, actor_schema)
	db.MustExecContext(ctx, log_schema)

	return db, nil
}

func max(x, y int) int {
	if x < y {
		return y
	}
	return x
}

func rotate(
	ctx context.Context,
	name, uuid string,
	db *sqlx.DB,
	rcnt, period int) {

	c := time.Tick(time.Duration(period) * time.Second)
	if c == nil {
		l.Logger.Error(
			"rotate error",
			zap.String("service", serviceName),
			zap.String("actor", name),
			zap.String("uuid", uuid),
			zap.String("error", fmt.Sprintf(
				"rotate period is invalid: %d", period)),
		)

		return
	}

	rating := make(chan struct{}, 1)

	selectSql := `SELECT seq, time, message FROM log ORDER BY seq LIMIT ? ;`
	deleteSql := `DELETE FROM log WHERE seq <= ? ;`
	selectCnt := `SELECT COUNT(*) FROM log ;`

	runner := func() {
		defer func() {
			<-rating
		}()

		var rowCnt int

		tx, err := db.BeginTxx(ctx, nil)
		if err != nil {
			l.Logger.Error(
				"backup db transaction error",
				zap.String("service", serviceName),
				zap.String("actor", name),
				zap.String("uuid", uuid),
				zap.String("error", err.Error()),
			)

			return
		}

		cntRow := tx.QueryRowxContext(ctx, selectCnt)
		cntRow.Scan(&rowCnt)

		if rowCnt > rcnt {
			rdb, err := initDB(ctx, name, uuid, DELETE, PRIVATE, rotateDB)
			if err != nil {
				l.Logger.Error(
					"rotate initDB error",
					zap.String("service", serviceName),
					zap.String("actor", name),
					zap.String("uuid", uuid),
					zap.String("error", err.Error()),
				)

				tx.Rollback()
				return
			}
			defer rdb.Close()

			rows, err := tx.QueryxContext(ctx, selectSql, rcnt)
			if err != nil {
				l.Logger.Error(
					"backup db query error",
					zap.String("service", serviceName),
					zap.String("actor", name),
					zap.String("uuid", uuid),
					zap.Int("row count", rcnt),
					zap.String("error", err.Error()),
				)

				tx.Rollback()
				return
			}
			defer rows.Close()

			lastSeq := 0

			for rows.Next() {
				var ll log
				if err := rows.StructScan(&ll); err != nil {
					l.Logger.Error(
						"backup db StructScan error",
						zap.String("service", serviceName),
						zap.String("actor", name),
						zap.String("uuid", uuid),
						zap.String("error", err.Error()),
					)

					tx.Rollback()
					return
				}

				_, err := rdb.NamedExecContext(
					ctx,
					insertLog,
					ll,
				)
				if err != nil {
					l.Logger.Error(
						"rotate db insert error",
						zap.String("service", serviceName),
						zap.String("actor", name),
						zap.String("uuid", uuid),
						zap.String("error", err.Error()),
					)

					tx.Rollback()
					return
				}

				lastSeq = ll.Seq
			}

			// Delete rotated data from backup DB
			_, err = tx.ExecContext(
				ctx,
				deleteSql,
				lastSeq,
			)
			if err != nil {
				l.Logger.Error(
					"backup db delete error",
					zap.String("service", serviceName),
					zap.String("actor", name),
					zap.String("uuid", uuid),
					zap.String("error", err.Error()),
				)

				tx.Rollback()
				return
			}
		}

		if err := tx.Commit(); err != nil {
			l.Logger.Error(
				"backup db commit error",
				zap.String("service", serviceName),
				zap.String("actor", name),
				zap.String("uuid", uuid),
				zap.String("error", err.Error()),
			)

			return
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-c:
			select {
			case rating <- struct{}{}:
				runner()
			default:
			}
		}
	}
}

func (s *Sqlite) Close() {
	if s.db != nil {
		s.db.Close()
	}
}

func (s *Sqlite) Insert(msg string) error {
	b, err := json.Marshal(message{Msg: msg})
	if err != nil {
		l.Logger.Error(
			"backup db insert error",
			zap.String("service", serviceName),
			zap.String("actor", s.name),
			zap.String("uuid", s.uuid),
			zap.String("error", err.Error()),
		)

		return err
	}

	_, err = s.db.NamedExecContext(
		s.ctx,
		insertLog,
		log{
			Time: time.Now().Format(time.RFC3339),
			Msg:  b,
		},
	)
	if err != nil {
		l.Logger.Error(
			"backup db insert error",
			zap.String("service", serviceName),
			zap.String("actor", s.name),
			zap.String("uuid", s.uuid),
			zap.String("error", err.Error()),
		)

		return err
	}

	return nil
}

func (s *Sqlite) Start(startTime time.Time) error {
	// use RFC3339 time format
	_, err := s.db.NamedExecContext(
		s.ctx,
		insertActorStart,
		actor{
			Uuid:  s.uuid,
			Name:  s.name,
			Stime: startTime.Format(time.RFC3339),
		},
	)
	if err != nil {
		l.Logger.Error(
			"backup db insert start time error",
			zap.String("service", serviceName),
			zap.String("actor", s.name),
			zap.String("uuid", s.uuid),
			zap.String("error", err.Error()),
		)

		return err
	}

	return nil
}

func (s *Sqlite) Stop(endTime time.Time) error {
	if s.db == nil {
		return ErrDbClosed
	}

	// use RFC3339 time format
	// Do not use context call due to actor is in cancel state
	_, err := s.db.NamedExec(
		updateActorEnd,
		actor{
			Uuid:  s.uuid,
			Etime: endTime.Format(time.RFC3339),
		},
	)
	if err != nil {
		l.Logger.Error(
			"backup db insert stop time error",
			zap.String("service", serviceName),
			zap.String("actor", s.name),
			zap.String("uuid", s.uuid),
			zap.String("error", err.Error()),
		)
		return err
	}

	return nil
}

// func main_test() {
// fmt.Println("starts")
// ctx, cancel := context.WithCancel(context.Background())

// quitSig := func() {
// cancel()
// }

// // register signal dispositions
// RegisterHandler(syscall.SIGQUIT, quitSig)
// RegisterHandler(syscall.SIGTERM, quitSig)
// RegisterHandler(syscall.SIGINT, quitSig)

// // db, _ := initDB(ctx, "actorX", "123456", DELETE, SHARED, backupDB)
// // rotate(ctx, "actorX", "123456", db, 30)

// s, err := NewSqlite(
// ctx,
// "actorX",
// "123456",
// DELETE,
// SHARED,
// 1000,
// 1,
// )
// if err != nil {
// fmt.Printf("error: %s\n", err.Error())
// }

// s.Start(time.Now())

// fmt.Println("HERE-1")

// func() {
// for {
// select {
// case <-ctx.Done():
// return
// default:
// s.Insert("hi there~")
// fmt.Println("HERE-2")
// // time.Sleep(12 * time.Second)
// }
// }
// }()

// fmt.Println("HERE-3")

// s.Stop(time.Now())
// fmt.Println("HERE-4")

// <-ctx.Done()
// fmt.Println("ends")
// }
