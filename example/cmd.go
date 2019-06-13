package main

import (
	"context"
	"fmt"
	"math/rand"
	"syscall"
	"time"

	"github.com/vsdmars/actor"
	"go.uber.org/zap"
)

func logActor(act actor.Actor) {
	for {
		select {
		case <-act.Done():
			fmt.Println("Sayonara, my friends~ -- logActor")
			return
		case v := <-act.Receive():
			fmt.Printf("LOG MESSAGE: %v\n", v)
			go act.Backup(string(v.(string)))
		}
	}
}

func pipe1Actor(act actor.Actor) {

	logActor, _ := actor.Get("logger")
	pipe2Actor, _ := actor.Get("pipe2")

	for {
		select {
		case <-act.Done():
			fmt.Println("Sayonara, my friends~ -- Pipe1Actor")
			return
		case msg := <-act.Receive():
			fmt.Printf("Pipe1 received message: %v\n", msg)
			go logActor.Send(fmt.Sprintf("pipe1 got message: %v", msg))
			go pipe2Actor.Send(msg)
		}
	}
}

func pipe2Actor(act actor.Actor) {

	logActor, _ := actor.Get("logger")

	for {
		select {
		case <-act.Done():
			fmt.Println("Sayonara, my friends~ -- Pipe2Actor")
			return
		case msg := <-act.Receive():
			fmt.Printf("Pipe2 received message: %v\n", msg)
			go logActor.Send(fmt.Sprintf("pipe2 got message: %v", msg))
		}
	}
}

func test(ctx context.Context) {

	pipe1, _ := actor.NewActor(ctx, "pipe1", 3, pipe1Actor, -1)
	actor.NewActor(ctx, "pipe2", 3, pipe2Actor, -1)

	for {
		select {
		case <-ctx.Done():
			return
		default:
			pipe1.Send(fmt.Sprintf("message %d", rand.Intn(100)))
			time.Sleep(1 * time.Second)
		}
	}

}

func main() {
	// setup log level
	actor.SetLogLevel(zap.FatalLevel)

	// create root context
	ctx, cancel := context.WithCancel(context.Background())

	quitSig := func() {
		cancel()
	}

	// register signal dispositions
	RegisterHandler(syscall.SIGQUIT, quitSig)
	RegisterHandler(syscall.SIGTERM, quitSig)
	RegisterHandler(syscall.SIGINT, quitSig)

	// create logging actor 'logger' and standby
	actor.NewActor(ctx, "logger", 3, logActor, 3)

	// starts test
	go test(ctx)

	<-ctx.Done()

	// Cleanup will call actor's close
	actor.Cleanup()

	// waits seconds for actors to safely clean up (graceful shutdown)
	// http://vsdmars.blogspot.com/2019/02/golangdesign-graceful-shutdown.html
	time.Sleep(3 * time.Second)
}
