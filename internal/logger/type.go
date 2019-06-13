package logger

import (
	"go.uber.org/zap"
)

type serviceLogger struct {
	*zap.Logger
	config   *zap.Config
	provided bool
}
