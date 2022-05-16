package hedwig

import (
	"context"
	"log"
)

// Logger represents a logging interface that this library expects - this is inspired by logur/go-kit
type Logger interface {
	// Error log with a message. `keyvals` can be used as additional metadata for structured logging.
	// You can generally expect one of these fields to be available: message_sqs_id, message_sns_id.
	Error(ctx context.Context, err error, message string, keyvals ...interface{})

	// Info logs a debug level log with a message. `keyvals` param works the same as `Error`.
	Info(ctx context.Context, message string, keyvals ...interface{})

	// Debug logs a debug level log with a message. `keyvals` param works the same as `Error`.
	Debug(ctx context.Context, message string, keyvals ...interface{})
}

type StdLogger struct{}

func (s StdLogger) toMap(keyvals ...interface{}) map[string]interface{} {
	if len(keyvals)%2 != 0 {
		// really a bug
		return nil
	}
	m := make(map[string]interface{}, len(keyvals)/2)
	for i := 0; i < len(keyvals); i += 2 {
		m[keyvals[i].(string)] = keyvals[i+1]
	}
	return m
}

func (s StdLogger) Error(_ context.Context, err error, message string, keyvals ...interface{}) {
	log.Printf("[ERROR] %s: %s %+v\n", message, err.Error(), s.toMap(keyvals))
}

func (s StdLogger) Info(_ context.Context, message string, keyvals ...interface{}) {
	log.Printf("[INFO] %s %+v\n", message, s.toMap(keyvals))
}

func (s StdLogger) Debug(_ context.Context, message string, keyvals ...interface{}) {
	log.Printf("[DEBUG] %s %+v\n", message, s.toMap(keyvals))
}
