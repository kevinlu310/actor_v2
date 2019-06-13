package actor

import (
	"fmt"

	l "github.com/vsdmars/actor/internal/logger"

	"go.uber.org/zap"
)

const (
	errDupRegister = "actor already registered by name: %s"
)

var (
	regActor = registeredActor{
		nameUUID:  make(map[string]string),
		uuidActor: make(map[string]Actor),
	}
)

// Cleanup cleans up the use of actor library
func Cleanup() {
	defer regActor.rwLock.RUnlock()
	regActor.rwLock.RLock()

	l.Logger.Info(
		"Actor Service Cleanup",
		zap.String("service", serviceName),
	)

	for _, actor := range regActor.uuidActor {
		actor.close()

		l.Logger.Info(
			"Actor closed due to Cleanup",
			zap.String("service", serviceName),
			zap.String("actor", actor.Name()),
			zap.String("uuid", actor.UUID()),
		)
	}

	l.LogSync()
}

// Get return registered Actor by name
func Get(name string) (Actor, error) {
	return regActor.getByName(name)
}

// GetByName return registered Actor by name
func GetByName(actor string) (Actor, error) {
	return Get(actor)
}

// GetByUUID return registered Actor by UUID
func GetByUUID(uuid string) (Actor, error) {
	return regActor.getByUUID(uuid)
}

func (r *registeredActor) register(actor Actor) error {
	defer r.rwLock.Unlock()
	r.rwLock.Lock()

	if _, ok := r.nameUUID[actor.Name()]; ok {
		l.Logger.Error(
			"register Actor failed",
			zap.String("service", serviceName),
			zap.String("actor", actor.Name()),
			zap.String("uuid", actor.UUID()),
			zap.String("error", fmt.Sprintf(errDupRegister, actor.Name())),
		)

		return ErrRegisterActor
	}

	r.nameUUID[actor.Name()] = actor.UUID()
	r.uuidActor[actor.UUID()] = actor

	l.Logger.Info(
		"actor registered",
		zap.String("service", serviceName),
		zap.String("actor", actor.Name()),
		zap.String("uuid", actor.UUID()),
	)

	return nil
}

func (r *registeredActor) deregister(actor Actor) error {
	defer r.rwLock.Unlock()
	r.rwLock.Lock()

	if _, ok := r.nameUUID[actor.Name()]; !ok {
		l.Logger.Error(
			"deregister Actor failed",
			zap.String("service", serviceName),
			zap.String("actor", actor.Name()),
			zap.String("uuid", actor.UUID()),
			zap.String("message", "actor haven't registered by name"),
		)

		return ErrRegisterActor
	}

	delete(r.uuidActor, actor.UUID())
	delete(r.nameUUID, actor.Name())

	l.Logger.Info(
		"actor deregistered",
		zap.String("service", serviceName),
		zap.String("actor", actor.Name()),
		zap.String("uuid", actor.UUID()),
	)

	return nil
}

func (r *registeredActor) getByName(name string) (Actor, error) {
	defer r.rwLock.RUnlock()
	r.rwLock.RLock()

	if uuid, ok := r.nameUUID[name]; ok {
		if actor, ok := r.uuidActor[uuid]; ok {
			l.Logger.Info(
				"get actor by name",
				zap.String("service", serviceName),
				zap.String("actor", name),
				zap.String("uuid", uuid),
				zap.String("message", "actor retrieved"),
			)

			return actor, nil
		}
	}

	l.Logger.Error(
		"get actor by name failed",
		zap.String("service", serviceName),
		zap.String("actor", name),
		zap.String("message", "actor not registered"),
	)

	return nil, ErrRetrieveActor
}

func (r *registeredActor) getByUUID(uuid string) (Actor, error) {
	defer r.rwLock.RUnlock()
	r.rwLock.RLock()

	if actor, ok := r.uuidActor[uuid]; ok {
		l.Logger.Info(
			"get actor by uuid",
			zap.String("service", serviceName),
			zap.String("uuid", uuid),
			zap.String("message", "actor retrieved"),
		)

		return actor, nil
	}

	l.Logger.Error(
		"get actor by uuid failed",
		zap.String("service", serviceName),
		zap.String("uuid", uuid),
		zap.String("message", "actor not registered"),
	)

	return nil, ErrRetrieveActor
}
