package hedwigzap

import (
	"context"

	"go.uber.org/zap"
)

type Logger struct{}

func (l *Logger) with(keyvals []interface{}) *zap.Logger {
	logger := zap.NewExample()
	if len(keyvals)%2 != 0 {
		// really a bug
		return logger
	}
	zfields := make([]zap.Field, 0, len(keyvals)/2)
	for i := 0; i < len(keyvals); i += 2 {
		zfields = append(zfields, zap.Reflect(keyvals[i].(string), keyvals[i+1]))
	}
	return logger.With(zfields...)
}

func (l Logger) Error(_ context.Context, err error, message string, keyvals ...interface{}) {
	l.with(keyvals).With(zap.Error(err)).Error(message)
}

func (l Logger) Info(_ context.Context, message string, keyvals ...interface{}) {
	l.with(keyvals).Info(message)
}

func (l Logger) Debug(_ context.Context, message string, keyvals ...interface{}) {
	l.with(keyvals).Debug(message)
}
