package contextlogger

import (
	"context"

	"github.com/cloudchacho/hedwig-go"
)

type Logger struct {
	hedwig.Logger
	Extractor func(ctx context.Context) []interface{}
}

func (l Logger) Error(ctx context.Context, err error, message string, keyvals ...interface{}) {
	keyvals = append(keyvals, l.Extractor(ctx))
	l.Logger.Error(ctx, err, message, keyvals...)
}

func (l Logger) Debug(ctx context.Context, message string, keyvals ...interface{}) {
	keyvals = append(keyvals, l.Extractor(ctx))
	l.Logger.Debug(ctx, message, keyvals...)
}
