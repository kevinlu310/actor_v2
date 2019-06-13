package actor

import (
	"context"
	"sync/atomic"
	"time"

	idb "github.com/vsdmars/actor/internal/db"
	l "github.com/vsdmars/actor/internal/logger"

	"github.com/google/uuid"
	"go.uber.org/zap"
)

// NewActor creates new local actor
//
// ctx: caller's context, able to cancel created actor
//
// name: actor's name
//
// buffer: actor's channel buffer
//
// callbackFn: actor handler
//
// b: < 0: disable backup, == 0: backup without rotation, > 0: backup with rotation rows
func NewActor(
	ctx context.Context, // caller's context, able to cancel created actor.
	name string, // actor's name
	buffer int, // actor's channel buffer
	callbackFn HandleType, // actor's handler
	b int, // backup actor's receiving message
) (Actor, error) {

	if buffer < 0 {
		return nil, ErrChannelBuffer
	}

	var db idb.DB
	uuidVal := uuid.New().String()

	if b >= 0 {
		// using sqlite for local backup
		s, err := idb.NewSqlite(
			ctx,
			name,
			uuidVal,
			idb.DELETE, // sqlite journal mode
			idb.SHARED, // sqlite cache mode
			b,          // rotate records
			30,         // rotate period/seconds
		)
		if err != nil {
			l.Logger.Error(
				"backup db creation error",
				zap.String("service", serviceName),
				zap.String("actor", name),
				zap.String("error", err.Error()),
			)

			return nil, err
		}

		db = s
	}

	// create Actor's context
	ctx, cancel := context.WithCancel(ctx)
	// create Actor's message channel
	pipe := make(chan interface{}, buffer)

	// escape localActor object store ptr to localActor instance into Actor interface
	actor := Actor(
		&localActor{
			name:         name,
			uuid:         uuidVal,
			actorContext: actorContext{ctx, cancel},
			channels:     channels{pipe, pipe},
			backup:       backup{db},
		},
	)

	if err := regActor.register(actor); err != nil {
		actor.close() // clean up actor

		l.Logger.Debug(
			"clean up duplicated actor",
			zap.String("service", serviceName),
			zap.String("actor", actor.Name()),
			zap.String("uuid", actor.UUID()),
			zap.String("error", err.Error()),
		)

		return nil, err
	}

	go func() {
		defer func() {
			regActor.deregister(actor)
			actor.endStamp()
			actor.close()

			if r := recover(); r != nil {
				l.Logger.Error(
					"actor handler panic",
					zap.String("service", serviceName),
					zap.String("actor", actor.Name()),
					zap.String("uuid", actor.UUID()),
					zap.Any("panic", r),
				)
			}
		}()

		actor.startStamp()

		go actor.increaseIdle()

		// block call
		// return closes the channel, actor dies
		callbackFn(actor)
	}()

	return actor, nil
}

// --- Actor interface functions ---

// Backup backups message into local sqlite db
func (actor *localActor) Backup(msg string) {
	if actor.db != nil {
		if err := actor.db.Insert(msg); err != nil {
			l.Logger.Error(
				"backup actor message error",
				zap.String("service", serviceName),
				zap.String("actor", actor.name),
				zap.String("uuid", actor.uuid),
				zap.String("error", err.Error()),
			)
		}
	}
}

// Done Actor's context.done()
//
// context.done() is used for cleaning up Actor resource
func (actor *localActor) Done() <-chan struct{} {
	return actor.ctx.Done()
}

// Idle returns actor's idle time
func (actor *localActor) Idle() time.Duration {
	return time.Duration(atomic.LoadInt64(&actor.idle))
}

// Name returns actor's name
func (actor *localActor) Name() string {
	return actor.name
}

// Receive receives message from actor
func (actor *localActor) Receive() <-chan interface{} {
	return actor.receive
}

// Send sends message to actor
func (actor *localActor) Send(message interface{}) (err error) {
	defer func() {
		if r := recover(); r != nil {
			l.Logger.Error(
				"actor in closed state",
				zap.String("service", serviceName),
				zap.String("actor", actor.name),
				zap.String("uuid", actor.uuid),
				zap.Any("recover", r),
			)

			err = ErrChannelClosed
		}
	}()

	select {
	case <-actor.Done():
		l.Logger.Error(
			"actor is cancelled",
			zap.String("service", serviceName),
			zap.String("actor", actor.name),
			zap.String("uuid", actor.uuid),
			zap.String("error", "actor is cancelled"),
		)

		err = ErrChannelClosed
		return
	default:
		// block, force golang scheduler to process message.
		// do not use select on purpose.
		actor.send <- message
		actor.resetIdle()

		l.Logger.Debug(
			"send",
			zap.String("service", serviceName),
			zap.String("actor", actor.Name()),
			zap.String("uuid", actor.UUID()),
			zap.Any("message", message),
		)

		return
	}
}

// UUID returns actor's UUID
func (actor *localActor) UUID() string {
	return actor.uuid
}

func (actor *localActor) close() {
	actor.cancel()

	// https://stackoverflow.com/a/8593986 Not a precise answer but ok.
	// do not close actor's channel avoid race condition
	// it's not a resource leak if channel remains open
	// Why? hey, hey, everything inside the channel is copied value
	// close(act.send)
}

func (actor *localActor) resetIdle() {
	atomic.StoreInt64(&actor.idle, 0)
}

func (actor *localActor) increaseIdle() {
	if actor.timer == nil {
		actor.timer = time.NewTimer(10 * time.Second)
	}

	for {
		select {
		case <-actor.Done():
			// clean up the timer
			if actor.timer != nil {
				actor.timer.Stop()
			}

			return
		case passed := <-actor.timer.C:
			atomic.AddInt64(
				&actor.idle,
				int64(time.Duration(passed.Second())*time.Second),
			)

			l.Logger.Debug(
				"actor idle seconds",
				zap.String("service", serviceName),
				zap.String("actor", actor.name),
				zap.String("uuid", actor.uuid),
				zap.Float64("seconds", time.Duration(
					atomic.LoadInt64(&actor.idle)).Seconds()),
			)

			actor.timer.Reset(10 * time.Second)
		}
	}
}

func (actor *localActor) startStamp() {
	actor.startTime = time.Now()

	if actor.db != nil {
		actor.db.Start(actor.startTime)
	}

	l.Logger.Info(
		"actor start time",
		zap.String("service", serviceName),
		zap.String("actor", actor.name),
		zap.String("uuid", actor.uuid),
		zap.String("time", actor.startTime.Format(time.UnixDate)),
	)
}

func (actor *localActor) endStamp() {
	actor.endTime = time.Now()

	if actor.db != nil {
		actor.db.Stop(actor.endTime)
		actor.db.Close()
	}

	l.Logger.Info(
		"actor end time",
		zap.String("service", serviceName),
		zap.String("actor", actor.name),
		zap.String("uuid", actor.uuid),
		zap.String("time", actor.endTime.Format(time.UnixDate)),
	)

}
