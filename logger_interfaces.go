package hedwig

import (
	"context"
	"log"

	"github.com/sirupsen/logrus"
)

type LoggingFields map[string]interface{}

// Logger represents an logging interface that this library expects
type Logger interface {
	// Error logs an error with a message. `fields` can be used as additional metadata for structured logging.
	// You can generally expect one of these fields to be available: message_sqs_id, message_sns_id.
	// By default fields are logged as a map using fmt.Sprintf
	Error(err error, message string, fields LoggingFields)

	// Warn logs a warn level log with a message. `fields` param works the same as `Error`.
	Warn(err error, message string, fields LoggingFields)

	// Info logs a debug level log with a message. `fields` param works the same as `Error`.
	Info(message string, fields LoggingFields)

	// Debug logs a debug level log with a message. `fields` param works the same as `Error`.
	Debug(message string, fields LoggingFields)
}

type logrusLogger struct {
	logrus.FieldLogger
}

func (l *logrusLogger) Error(err error, message string, fields LoggingFields) {
	l.WithError(err).WithFields(logrus.Fields(fields)).Error(message)
}

func (l *logrusLogger) Warn(err error, message string, fields LoggingFields) {
	l.WithError(err).WithFields(logrus.Fields(fields)).Warn(message)
}

func (l *logrusLogger) Info(message string, fields LoggingFields) {
	l.WithFields(logrus.Fields(fields)).Info(message)
}

func (l *logrusLogger) Debug(message string, fields LoggingFields) {
	l.WithFields(logrus.Fields(fields)).Debug(message)
}

func LogrusGetLoggerFunc(fn func(ctx context.Context) *logrus.Entry) GetLoggerFunc {
	return func(ctx context.Context) Logger {
		return &logrusLogger{fn(ctx)}
	}
}

type StdLogger struct{}

func (s *StdLogger) Error(err error, message string, fields LoggingFields) {
	log.Printf("[ERROR] %s [error: %+v][fields: %+v]\n", message, err, fields)
}

func (s *StdLogger) Warn(err error, message string, fields LoggingFields) {
	log.Printf("[WARN] %s [error: %+v][fields: %+v]\n", message, err, fields)
}

func (s *StdLogger) Info(message string, fields LoggingFields) {
	log.Printf("[INFO] %s [fields: %+v]\n", message, fields)
}

func (s *StdLogger) Debug(message string, fields LoggingFields) {
	log.Printf("[DEBUG] %s [fields: %+v]\n", message, fields)
}
