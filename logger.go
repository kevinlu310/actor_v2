package actor

import (
	l "github.com/vsdmars/actor/internal/logger"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// SetLogger sets caller provided zap logger
//
// reset to service's default logger by passing in nil pointer
func SetLogger(ol *zap.Logger) {
	l.SetLogger(ol)
}

// SetLogLevel sets the service log level
//
// noop if caller provides it's own zap logger
func SetLogLevel(level zapcore.Level) {
	l.SetLogLevel(level)
}
