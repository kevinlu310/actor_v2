package actor

import "errors"

var (
	// ErrChannelBuffer channel buffer setting error
	ErrChannelBuffer = errors.New("channel buffer error")
	// ErrChannelClosed channel is in closed state
	ErrChannelClosed = errors.New("channel in closed state error")
	// ErrRegisterActor register actor error
	ErrRegisterActor = errors.New("register actor error")
	// ErrRetrieveActor retrieve actor error
	ErrRetrieveActor = errors.New("retrieve actor error")
	// ErrSend actor send message error
	ErrSend = errors.New("send message error")
)
