package actor

import (
	"context"
	"sync"
	"time"

	idb "github.com/vsdmars/actor/internal/db"
)

type (
	actorContext struct {
		ctx    context.Context // clean up by .cancel it
		cancel context.CancelFunc
	}

	backup struct {
		db idb.DB
	}

	channels struct {
		send    chan<- interface{} // clean up by .close it
		receive <-chan interface{} // clean up by .close it
	}

	timing struct {
		startTime time.Time
		endTime   time.Time
		timer     *time.Timer // clean up by .Stop it
		// timer atomic.Value // stores *time.Timer instance, clean up by .Stop it
		idle int64
	}
)

type (
	// Actor provides several member functions to interact with Actor
	localActor struct {
		name string
		uuid string
		actorContext
		timing
		channels
		backup
	}

	remoteActor struct {
		name string
		uuid string
		actorContext
		timing
	}
)

type (
	registeredActor struct {
		rwLock    sync.RWMutex
		nameUUID  map[string]string
		uuidActor map[string]Actor
	}
)

type (
	// Actor is the actor interface for client to refer to
	Actor interface {
		Name() string
		UUID() string
		Idle() time.Duration
		Send(message interface{}) error
		Receive() <-chan interface{}
		Done() <-chan struct{}
		Backup(string)
		close()        // close actor channel
		resetIdle()    // reset actor idle duration
		increaseIdle() // increase actor idle duration
		startStamp()
		endStamp()
	}
	// HandleType is the actor handle function signature
	HandleType func(Actor)
)
