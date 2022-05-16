package hedwiglogrus

import (
	"context"

	"github.com/sirupsen/logrus"
)

type Logger struct{}

func (l Logger) withFields(keyvals []interface{}) logrus.FieldLogger {
	entry := logrus.NewEntry(logrus.New())
	if len(keyvals)%2 == 0 {
		for i := 0; i < len(keyvals); i += 2 {
			entry = entry.WithField(keyvals[i].(string), keyvals[i+1])
		}
	}
	return entry
}

func (l Logger) Error(_ context.Context, err error, message string, keyvals ...interface{}) {
	l.withFields(keyvals).WithError(err).Error(message)
}

func (l Logger) Info(_ context.Context, message string, keyvals ...interface{}) {
	l.withFields(keyvals).Info(message)
}

func (l Logger) Debug(_ context.Context, message string, keyvals ...interface{}) {
	l.withFields(keyvals).Debug(message)
}
