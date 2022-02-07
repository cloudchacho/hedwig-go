package hedwig

import (
	"testing"
	"time"

	"github.com/Masterminds/semver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeHedwigDataField is a fake data field for testing
type fakeHedwigDataField struct {
	VehicleID string `json:"vehicle_id"`
}

func TestCreateMetadata(t *testing.T) {
	assertions := assert.New(t)

	now := time.Now()
	headers := map[string]string{"X-Request-Id": "abc123"}

	metadata := createMetadata(headers, "myapp")
	assertions.NotNil(metadata)

	assertions.Equal(headers, metadata.Headers)
	assertions.Equal("myapp", metadata.Publisher)
	assertions.Nil(metadata.ProviderMetadata)
	assertions.True(metadata.Timestamp.UnixNano() >= now.UnixNano())
}

func TestNewMessageWithIDSuccess(t *testing.T) {
	assertions := assert.New(t)

	headers := map[string]string{"X-Request-Id": "abc123"}
	metadata := createMetadata(headers, "myapp")
	assertions.NotNil(metadata)

	id := "abcdefgh"
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"
	data := fakeHedwigDataField{}

	m, err := newMessageWithID(id, msgDataType, msgDataSchemaVersion, metadata, &data)
	require.NoError(t, err)

	assertions.Equal(data, *m.Data.(*fakeHedwigDataField))
	assertions.Equal(headers, metadata.Headers)
	assertions.Equal(id, m.ID)
	assertions.Equal(msgDataType, m.Type)
	ver, err := semver.NewVersion(msgDataSchemaVersion)
	assertions.NoError(err)
	assertions.Equal(ver, m.DataSchemaVersion)
}

func TestNewMessageWithIDEmptySchemaVersion(t *testing.T) {
	assertions := assert.New(t)

	headers := map[string]string{"X-Request-Id": "abc123"}
	metadata := createMetadata(headers, "myapp")
	assertions.NotNil(metadata)

	id := "abcdefgh"
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := ""

	_, err := newMessageWithID(id, msgDataType, msgDataSchemaVersion, metadata, &fakeHedwigDataField{})
	assertions.NotNil(err)
}

func TestNewMessageWithIDInvalidSchemaVersion(t *testing.T) {
	assertions := assert.New(t)

	headers := map[string]string{"X-Request-Id": "abc123"}
	metadata := createMetadata(headers, "myapp")
	assertions.NotNil(metadata)

	id := "abcdefgh"
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "a.b"

	_, err := newMessageWithID(id, msgDataType, msgDataSchemaVersion, metadata, &fakeHedwigDataField{})
	assertions.NotNil(err)
}

func TestNewMessageWithIDNilData(t *testing.T) {
	assertions := assert.New(t)

	headers := map[string]string{"X-Request-Id": "abc123"}
	metadata := createMetadata(headers, "myapp")
	assertions.NotNil(metadata)

	id := "abcdefgh"
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"

	_, err := newMessageWithID(id, msgDataType, msgDataSchemaVersion, metadata, nil)
	assertions.NotNil(err)
}

func TestNewMessage(t *testing.T) {
	assertions := assert.New(t)

	headers := map[string]string{"X-Request-Id": "abc123"}
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"
	data := fakeHedwigDataField{}

	m, err := NewMessage(msgDataType, msgDataSchemaVersion, headers, &data, "myapp")
	require.NoError(t, err)

	assertions.Equal(data, *m.Data.(*fakeHedwigDataField))
	assertions.True(len(m.ID) > 0 && len(m.ID) < 40)
	assertions.Equal(headers, m.Metadata.Headers)
	ver, err := semver.NewVersion(msgDataSchemaVersion)
	assertions.NoError(err)
	assertions.Equal(ver, m.DataSchemaVersion)
	assertions.Equal(msgDataType, m.Type)
}
