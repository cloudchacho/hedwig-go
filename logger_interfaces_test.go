package hedwig_test

import (
	"context"
	"errors"
	"io"
	"log"
	"os"
	"testing"

	"github.com/cloudchacho/hedwig-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func Test_StdLogger(t *testing.T) {
	tmpDir := t.TempDir()
	tests := []struct {
		name  string
		fcall func(logger hedwig.StdLogger)
		want  string
	}{
		{
			name: "Debug",
			fcall: func(logger hedwig.StdLogger) {
				var ctx context.Context
				logger.Debug(ctx, "Test message", "key", "value", "key2", "value2")
			},
			want: "[DEBUG] Test message map[key:value key2:value2]",
		},
		{
			name: "Error",
			fcall: func(logger hedwig.StdLogger) {
				var ctx context.Context
				logger.Error(ctx, errors.New("oops!"), "Test message", "key", "value", "key2", "value2")
			},
			want: "[ERROR] Test message: oops! map[key:value key2:value2]",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tmpFile, err := os.CreateTemp(tmpDir, "stderr")
			require.NoError(t, err)
			log.Default().SetOutput(tmpFile)

			t.Cleanup(func() {
				log.Default().SetOutput(os.Stderr)
			})
			logger := hedwig.StdLogger{}
			test.fcall(logger)
			err = tmpFile.Close()
			require.NoError(t, err)
			tmpFile, err = os.Open(tmpFile.Name())
			require.NoError(t, err)
			t.Cleanup(func() {
				err = tmpFile.Close()
				require.NoError(t, err)
			})
			contents, err := io.ReadAll(tmpFile)
			require.NoError(t, err)
			assert.Contains(t, string(contents), test.want)
		})
	}
}
