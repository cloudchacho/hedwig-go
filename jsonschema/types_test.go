package jsonschema_test

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/cloudchacho/hedwig-go/jsonschema"
)

func TestJSONTimeMarshal(t *testing.T) {
	ts := jsonschema.JSONTime(time.Unix(1621870411, 123456000))
	marshalled, err := json.Marshal(ts)
	require.NoError(t, err)
	assert.Equal(t, []byte(`1621870411123`), marshalled)
}

func TestJSONTimeUnmarshal(t *testing.T) {
	ts := jsonschema.JSONTime{}
	err := json.Unmarshal([]byte(`1621870411123`), &ts)
	require.NoError(t, err)
	assert.Equal(t, jsonschema.JSONTime(time.Unix(1621870411, 123000000)), ts)
}

func TestJSONTimeUnmarshalFailure(t *testing.T) {
	ts := jsonschema.JSONTime{}
	err := json.Unmarshal([]byte(`"2021-05-24T11:23:47-0700"`), &ts)
	assert.Error(t, err)
}
