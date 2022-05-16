package hedwigzerolog

import (
	"context"

	"github.com/rs/zerolog/log"
)

type Logger struct{}

func (l Logger) Error(_ context.Context, err error, message string, keyvals ...interface{}) {
	log.Err(err).Fields(keyvals).Msg(message)
}

func (l Logger) Info(_ context.Context, message string, keyvals ...interface{}) {
	log.Info().Fields(keyvals).Msg(message)
}

func (l Logger) Debug(_ context.Context, message string, keyvals ...interface{}) {
	log.Debug().Fields(keyvals).Msg(message)
}
