package actor_test

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/eapache/go-resiliency/breaker"
	"github.com/eapache/go-resiliency/deadline"
	"github.com/google/uuid"
	"github.com/vsdmars/actor"
	"go.uber.org/zap"
)

type testCase struct {
	name   string // actor name
	buffer int    // actor buffer
	backup int    // actor backup
}

const (
	actorBufferErr    = "actor buffer error"
	actorIdleErr      = "actor has incorrect idle time: %v seconds, expecting: %s"
	actorNotClosed    = "actor not closed while cancelled"
	actorNotCleanup   = "actor not cleanup while panic"
	actorPanicErr     = "actor panic error"
	actorTimeoutErr   = "actor timeout error"
	createActorErr    = "create actor error"
	duplicateActorErr = "duplicate actor error"
	getActorErr       = "get actor error"
	retrieveActorErr  = "retrieve actor error: %s"
)

var (
	errActorCleanup    = errors.New("actor cleanup")
	errActorNotCleanup = errors.New("actor not cleanup yet")
	errActorTimeout    = errors.New("actor timeout")
)

var actorCnt uint64
var msgCnt uint64

func init() {
	flag.Uint64Var(&actorCnt, "actor", 100, "number of actors")
	flag.Uint64Var(&msgCnt, "msg", 42000, "number of messages")

	// setup log level
	actor.SetLogLevel(zap.FatalLevel)
}

func createHandle(
	t *testing.T,
	expect string, // expecting message receives
	cnt int, // expecting number of message receives
	p bool, // panic
) (func(actor.Actor), <-chan struct{}) {

	done := make(chan struct{})

	handle := func(act actor.Actor) {
		if p {
			panic(actorPanicErr)
		}

		checker := func(stopper <-chan struct{}) error {
			count := 0

			for {
				select {
				case <-stopper:
					return errActorTimeout
				case <-act.Done():
					close(done)
					return errActorCleanup
				case d := <-act.Receive():
					s, ok := d.(string)
					if !ok {
						t.Error("receiving message with wrong type")
					}

					if s != expect {
						t.Errorf("expecting: %s, receiving: %s", expect, s)
					}

					count += 1
					if count == cnt {
						close(done)
						return nil
					}
				}
			}
		}

		dl := deadline.New(2 * time.Minute)
		err := dl.Run(checker)
		switch err {
		case deadline.ErrTimedOut:
			// execution took too long, oops
			t.Error(actorTimeoutErr)
			close(done)
		}
	}

	return handle, done
}

func createTestCase(number int, buffer, backup int) []testCase {
	tc := make([]testCase, number)
	name := "actor_%d_%s"

	for idx := 0; idx < number; idx++ {
		tc[idx] = testCase{
			fmt.Sprintf(name, idx, uuid.New().String()),
			buffer,
			backup,
		}
	}

	return tc
}

func TestSingleActor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msg := "i am the lead role!"

	for _, tc := range createTestCase(1, 0, -1) {
		handle, done := createHandle(t, msg, 3, false)

		act, err := actor.NewActor(
			ctx,
			tc.name,
			tc.buffer,
			handle,
			tc.backup,
		)
		if err != nil {
			t.Fatal(createActorErr)
		}

		act.Send(msg)
		act.Send(msg)
		act.Send(msg)

		<-done
	}
}

func TestSingleActorBurst(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msg := "i am the lead role!"
	burst := int(msgCnt)

	for _, tc := range createTestCase(1, 3, -1) {
		handle, done := createHandle(t, msg, burst, false)

		act, err := actor.NewActor(
			ctx,
			tc.name,
			tc.buffer,
			handle,
			tc.backup,
		)
		if err != nil {
			t.Fatal(createActorErr)
		}

		for idx := 0; idx < burst; idx++ {
			act.Send(msg)
		}

		<-done
	}
}

func TestSingleActorMultiSend(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping SingleActorMultiSend in short mode.")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msg := "i am the lead role!"
	burst := int(msgCnt)

	sender := func(name string, number int) {
		act, err := actor.Get(name)
		if err != nil {
			t.Error(getActorErr)
			return
		}

		for idx := 0; idx < number; idx++ {
			act.Send(msg)
		}
	}

	for _, tc := range createTestCase(1, 3, -1) {
		handle, done := createHandle(t, msg, burst, false)

		act, err := actor.NewActor(
			ctx,
			tc.name,
			tc.buffer,
			handle,
			tc.backup,
		)
		if err != nil {
			t.Fatal(createActorErr)
		}

		dispatch := burst
		for dispatch > 0 {
			n := int(rand.Uint32()) % dispatch
			if n == 0 {
				n = dispatch
			}

			go sender(act.Name(), n)

			dispatch -= n
		}

		<-done
	}
}

func TestMultiActorMultiSend(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping MultiActorMultiSend in short mode.")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msg := "i am the lead role!"
	burst := int(msgCnt)
	actors := int(actorCnt)

	sender := func(name string, number int) {
		act, err := actor.Get(name)
		if err != nil {
			t.Error(getActorErr)
			return
		}

		for idx := 0; idx < number; idx++ {
			act.Send(msg)
		}
	}

	t.Run("MultiActorMultiSend",
		func(t *testing.T) {
			for idx, tc := range createTestCase(actors, 3, -1) {
				tc := tc

				h := func(t *testing.T) {
					t.Parallel()

					handle, done := createHandle(t, msg, burst, false)

					act, err := actor.NewActor(
						ctx,
						tc.name,
						tc.buffer,
						handle,
						tc.backup,
					)
					if err != nil {
						t.Fatal(createActorErr)
					}

					dispatch := burst
					for dispatch > 0 {
						n := int(rand.Uint32()) % dispatch
						if n == 0 {
							n = dispatch
						}

						go sender(act.Name(), n)

						dispatch -= n
					}

					<-done
				}

				t.Run(fmt.Sprintf("MultiActorMultiSend_%d", idx), h)
			}
		},
	)
}

func TestPanicActor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msg := "i am the lead role!"

	for _, tc := range createTestCase(1, 3, -1) {
		handle, _ := createHandle(t, msg, 3, true)

		_, err := actor.NewActor(
			ctx,
			tc.name,
			tc.buffer,
			handle,
			tc.backup,
		)
		if err != nil {
			t.Fatal(createActorErr)
		}

		checker := func() error {
			_, err = actor.Get(tc.name)
			if err != actor.ErrRetrieveActor {
				time.Sleep(3 * time.Second)
				return errActorNotCleanup
			}

			return nil
		}

		b := breaker.New(10, 1, 5*time.Second)
		for {
			result := b.Run(checker)

			switch result {
			case nil:
				// success!
				return
			case breaker.ErrBreakerOpen:
				t.Error(actorNotCleanup)
				return
			}
		}
	}
}

func TestActorBuffer(t *testing.T) {
	for _, tc := range createTestCase(1, -1, -1) {
		handle, _ := createHandle(t, "", 3, false)

		_, err := actor.NewActor(
			context.Background(),
			tc.name,
			tc.buffer,
			handle,
			tc.backup,
		)
		if err != actor.ErrChannelBuffer {
			t.Error(createActorErr)
		}
	}
}

func TestActorIdle(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping MultiActorMultiSend in short mode.")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msg := "i am the lead role!"

	for _, tc := range createTestCase(1, 0, -1) {
		handle, done := createHandle(t, msg, 1, false)

		act, err := actor.NewActor(
			ctx,
			tc.name,
			tc.buffer,
			handle,
			tc.backup,
		)
		if err != nil {
			t.Fatal(createActorErr)
		}

		// actor runtime updates idle timer every 10 seconds
		time.Sleep(30 * time.Second)
		ret := act.Idle().Seconds()
		if ret <= 9 {
			t.Errorf(actorIdleErr, ret, ">= 9 seconds")
		}

		act.Send(msg)
		ret = act.Idle().Seconds()
		if ret > 9 {
			t.Errorf(actorIdleErr, ret, "< 9 seconds")
		}

		<-done
	}
}

func TestSendToClosedActor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	msg := "i am the lead role!"

	for _, tc := range createTestCase(1, 0, -1) {
		handle, done := createHandle(t, msg, 1, false)

		act, err := actor.NewActor(
			ctx,
			tc.name,
			tc.buffer,
			handle,
			tc.backup,
		)
		if err != nil {
			t.Fatal(createActorErr)
		}

		cancel()

		err = act.Send(msg)
		if err != actor.ErrChannelClosed {
			t.Error(actorNotClosed)
		}

		<-done
	}
}

func TestCleanupActors(t *testing.T) {
	ctx, _ := context.WithCancel(context.Background())

	msg := "i am the lead role!"
	var actors []actor.Actor

	for _, tc := range createTestCase(100, 0, -1) {
		handle, _ := createHandle(t, msg, 1, false)

		act, err := actor.NewActor(
			ctx,
			tc.name,
			tc.buffer,
			handle,
			tc.backup,
		)
		if err != nil {
			t.Fatal(createActorErr)
		}

		actors = append(actors, act)
	}

	actor.Cleanup()

	for _, act := range actors {
		err := act.Send(msg)
		if err != actor.ErrChannelClosed {
			t.Error(actorNotClosed)
		}
	}
}

func TestGetByNameAndUUID(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msg := "i am the lead role!"

	for _, tc := range createTestCase(100, 0, -1) {
		handle, done := createHandle(t, msg, 3, false)

		act, err := actor.NewActor(
			ctx,
			tc.name,
			tc.buffer,
			handle,
			tc.backup,
		)
		if err != nil {
			t.Fatal(createActorErr)
		}

		act, err = actor.GetByUUID(act.UUID())
		act.Send(msg)
		act.Send(msg)
		act.Send(msg)

		<-done
	}

	_, err := actor.GetByUUID("GARBAGE")
	if err != actor.ErrRetrieveActor {
		t.Errorf(retrieveActorErr, "By UUID")
	}

	_, err = actor.Get("GARBAGE")
	if err != actor.ErrRetrieveActor {
		t.Errorf(retrieveActorErr, "By Name")
	}
}

func TestDuplicateActor(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msg := "i am the lead role!"
	sameActorName := "vsdmars"

	handle, _ := createHandle(t, msg, 3, false)

	_, err := actor.NewActor(
		ctx,
		sameActorName,
		0,
		handle,
		-1,
	)
	if err != nil {
		t.Fatal(createActorErr)
	}

	_, err = actor.NewActor(
		ctx,
		sameActorName,
		0,
		handle,
		-1,
	)
	if err != actor.ErrRegisterActor {
		t.Error(duplicateActorErr)
	}
}
