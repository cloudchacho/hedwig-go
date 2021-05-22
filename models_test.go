/*
 * Author: Michael Ngo
 */

package hedwig

import (
	"testing"
	"time"

	"github.com/Masterminds/semver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// FakeHedwigDataField is a fake data field for testing
type FakeHedwigDataField struct {
	VehicleID string `json:"vehicle_id"`
}

func createTestSettings() *Settings {
	s := &Settings{
		AWSRegion:     "us-east-1",
		AWSAccountID:  "1234567890",
		PublisherName: "myapp",
		QueueName:     "DEV-MYAPP",
	}
	s.initDefaults()
	return s
}

func TestCreateMetadata(t *testing.T) {
	assertions := assert.New(t)

	now := time.Now()
	headers := map[string]string{"X-Request-Id": "abc123"}
	settings := createTestSettings()

	metadata, err := createMetadata(settings, headers)
	assertions.NotNil(metadata)
	require.NoError(t, err)

	assertions.Equal(headers, metadata.Headers)
	assertions.Equal(settings.PublisherName, metadata.Publisher)
	assertions.Nil(metadata.ProviderMetadata)
	assertions.True(time.Time(metadata.Timestamp).UnixNano() >= now.UnixNano())
}

func TestNewMessageWithIDSuccess(t *testing.T) {
	assertions := assert.New(t)

	headers := map[string]string{"X-Request-Id": "abc123"}
	settings := createTestSettings()
	metadata, err := createMetadata(settings, headers)
	assertions.NotNil(metadata)
	require.NoError(t, err)

	id := "abcdefgh"
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"
	data := FakeHedwigDataField{}

	m, err := newMessageWithID(settings, id, msgDataType, msgDataSchemaVersion, metadata, &data)
	require.NoError(t, err)

	assertions.Equal(data, *m.Data.(*FakeHedwigDataField))
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
	settings := createTestSettings()
	metadata, err := createMetadata(settings, headers)
	assertions.NotNil(metadata)
	require.NoError(t, err)

	id := "abcdefgh"
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := ""

	_, err = newMessageWithID(settings, id, msgDataType, msgDataSchemaVersion, metadata, &FakeHedwigDataField{})
	assertions.NotNil(err)
}

func TestNewMessageWithIDInvalidSchemaVersion(t *testing.T) {
	assertions := assert.New(t)

	headers := map[string]string{"X-Request-Id": "abc123"}
	settings := createTestSettings()
	metadata, err := createMetadata(settings, headers)
	assertions.NotNil(metadata)
	require.NoError(t, err)

	id := "abcdefgh"
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "a.b"

	_, err = newMessageWithID(settings, id, msgDataType, msgDataSchemaVersion, metadata, &FakeHedwigDataField{})
	assertions.NotNil(err)
}

func TestNewMessageWithIDNilData(t *testing.T) {
	assertions := assert.New(t)

	headers := map[string]string{"X-Request-Id": "abc123"}
	settings := createTestSettings()
	metadata, err := createMetadata(settings, headers)
	assertions.NotNil(metadata)
	require.NoError(t, err)

	id := "abcdefgh"
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"

	_, err = newMessageWithID(settings, id, msgDataType, msgDataSchemaVersion, metadata, nil)
	assertions.NotNil(err)
}

func TestNewMessage(t *testing.T) {
	assertions := assert.New(t)

	headers := map[string]string{"X-Request-Id": "abc123"}
	settings := createTestSettings()
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"
	data := FakeHedwigDataField{}

	m, err := NewMessage(settings, msgDataType, msgDataSchemaVersion, headers, &data)
	require.NoError(t, err)

	assertions.Equal(data, *m.Data.(*FakeHedwigDataField))
	assertions.True(len(m.ID) > 0 && len(m.ID) < 40)
	assertions.Equal(headers, m.Metadata.Headers)
	ver, err := semver.NewVersion(msgDataSchemaVersion)
	assertions.NoError(err)
	assertions.Equal(ver, m.DataSchemaVersion)
	assertions.Equal(msgDataType, m.Type)
}
