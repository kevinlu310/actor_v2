package logger

import (
	"fmt"
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Logger is the zap logging service
var Logger serviceLogger
var origLogger serviceLogger

func init() {
	initLogger()
}

// LogSync sync logger output
func LogSync() {
	// ignore logger Sync error
	Logger.Sync()
}

// SetLogger sets caller provided zap logger
//
// reset to service's default logger by passing in nil pointer
func SetLogger(l *zap.Logger) {
	if l != nil {
		Logger.Logger = l
		Logger.provided = true
		return
	}

	Logger = origLogger
}

// SetLogLevel sets the service log level
//
// noop if caller provides it's own zap logger
func SetLogLevel(level zapcore.Level) {
	if Logger.provided {
		return
	}

	Logger.config.Level.SetLevel(level)
}

func initLogger() {
	// default log level set to 'info'
	atom := zap.NewAtomicLevelAt(zap.InfoLevel)

	config := zap.Config{
		Level:       atom,
		Development: false,
		Sampling: &zap.SamplingConfig{
			Initial:    100,
			Thereafter: 100,
		},
		Encoding:         "json", // console, json, toml
		EncoderConfig:    zap.NewProductionEncoderConfig(),
		OutputPaths:      []string{"stderr"},
		ErrorOutputPaths: []string{"stderr"},
	}

	mylogger, err := config.Build()
	if err != nil {
		fmt.Printf("Initialize zap logger error: %v\n", err)
		os.Exit(1)
	}

	Logger = serviceLogger{mylogger, &config, false}
	origLogger = Logger
}
