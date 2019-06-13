// +build !database

package db

import (
	"context"
	"time"

	l "github.com/vsdmars/actor/internal/logger"
	"go.uber.org/zap"
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
	return &Sqlite{
		ctx:     ctx,
		name:    name,
		uuid:    uuid,
		journal: jmode,
		cache:   cmode,
		rotate:  rcnt,
		db:      nil,
	}, nil
}

func (s *Sqlite) Close() {
	l.Logger.Debug(
		"Close noop",
		zap.String("service", serviceName),
		zap.String("actor", s.name),
		zap.String("uuid", s.uuid),
	)
}

func (s *Sqlite) Insert(msg string) error {
	l.Logger.Debug(
		"Insert noop",
		zap.String("service", serviceName),
		zap.String("actor", s.name),
		zap.String("uuid", s.uuid),
		zap.String("message", msg),
	)

	return nil
}

func (s *Sqlite) Start(startTime time.Time) error {
	l.Logger.Debug(
		"Start noop",
		zap.String("service", serviceName),
		zap.String("actor", s.name),
		zap.String("uuid", s.uuid),
		zap.String("start time", startTime.String()),
	)

	return nil
}

func (s *Sqlite) Stop(endTime time.Time) error {
	l.Logger.Debug(
		"Stop noop",
		zap.String("service", serviceName),
		zap.String("actor", s.name),
		zap.String("uuid", s.uuid),
		zap.String("end time", endTime.String()),
	)

	return nil
}
