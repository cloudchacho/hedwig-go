/*
 * Author: Michael Ngo
 */

package hedwig

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"

	"github.com/Masterminds/semver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeHedwigDataField is a fake data field for testing
type fakeHedwigDataField struct {
	VehicleID string `json:"vehicle_id"`
}

type fakePublisher struct {
	mock.Mock
}

func (f *fakePublisher) Publish(ctx context.Context, message *Message) (string, error) {
	args := f.Called(ctx, message)
	return args.String(0), args.Error(1)
}

func (f *fakePublisher) WithInstrumenter(instrumenter Instrumenter) IPublisher {
	return f
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

	metadata := createMetadata(settings, headers)
	assertions.NotNil(metadata)

	assertions.Equal(headers, metadata.Headers)
	assertions.Equal(settings.PublisherName, metadata.Publisher)
	assertions.Nil(metadata.ProviderMetadata)
	assertions.True(time.Time(metadata.Timestamp).UnixNano() >= now.UnixNano())
}

func TestNewMessageWithIDSuccess(t *testing.T) {
	assertions := assert.New(t)

	headers := map[string]string{"X-Request-Id": "abc123"}
	settings := createTestSettings()
	metadata := createMetadata(settings, headers)
	assertions.NotNil(metadata)

	id := "abcdefgh"
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"
	data := fakeHedwigDataField{}

	m, err := newMessageWithID(settings, id, msgDataType, msgDataSchemaVersion, metadata, &data)
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
	settings := createTestSettings()
	metadata := createMetadata(settings, headers)
	assertions.NotNil(metadata)

	id := "abcdefgh"
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := ""

	_, err := newMessageWithID(settings, id, msgDataType, msgDataSchemaVersion, metadata, &fakeHedwigDataField{})
	assertions.NotNil(err)
}

func TestNewMessageWithIDInvalidSchemaVersion(t *testing.T) {
	assertions := assert.New(t)

	headers := map[string]string{"X-Request-Id": "abc123"}
	settings := createTestSettings()
	metadata := createMetadata(settings, headers)
	assertions.NotNil(metadata)

	id := "abcdefgh"
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "a.b"

	_, err := newMessageWithID(settings, id, msgDataType, msgDataSchemaVersion, metadata, &fakeHedwigDataField{})
	assertions.NotNil(err)
}

func TestNewMessageWithIDNilData(t *testing.T) {
	assertions := assert.New(t)

	headers := map[string]string{"X-Request-Id": "abc123"}
	settings := createTestSettings()
	metadata := createMetadata(settings, headers)
	assertions.NotNil(metadata)

	id := "abcdefgh"
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"

	_, err := newMessageWithID(settings, id, msgDataType, msgDataSchemaVersion, metadata, nil)
	assertions.NotNil(err)
}

func TestMessageSerialize(t *testing.T) {
	settings := createTestSettings()
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"
	data := fakeHedwigDataField{}
	m, err := NewMessage(settings, msgDataType, msgDataSchemaVersion, nil, &data)
	require.NoError(t, err)

	validator := &fakeValidator{}
	payload := []byte(`{"type": "user-created"}`)
	headers := map[string]string{}

	validator.On("Serialize", m).
		Return(payload, headers, nil)

	returnedPayload, returnedHeaders, err := m.Serialize(validator)
	assert.NoError(t, err)
	assert.Equal(t, payload, returnedPayload)
	assert.Equal(t, headers, returnedHeaders)

	validator.AssertExpectations(t)
}

func TestMessagePublish(t *testing.T) {
	ctx := context.Background()

	settings := createTestSettings()
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"
	data := fakeHedwigDataField{}
	m, err := NewMessage(settings, msgDataType, msgDataSchemaVersion, nil, &data)
	require.NoError(t, err)

	publisher := &fakePublisher{}
	messageID := "123"

	publisher.On("Publish", ctx, m).
		Return(messageID, nil)

	returnedmessageID, err := m.Publish(ctx, publisher)
	assert.NoError(t, err)
	assert.Equal(t, messageID, returnedmessageID)

	publisher.AssertExpectations(t)
}

func TestNewMessage(t *testing.T) {
	assertions := assert.New(t)

	headers := map[string]string{"X-Request-Id": "abc123"}
	settings := createTestSettings()
	msgDataType := "vehicle_created"
	msgDataSchemaVersion := "1.0"
	data := fakeHedwigDataField{}

	m, err := NewMessage(settings, msgDataType, msgDataSchemaVersion, headers, &data)
	require.NoError(t, err)

	assertions.Equal(data, *m.Data.(*fakeHedwigDataField))
	assertions.True(len(m.ID) > 0 && len(m.ID) < 40)
	assertions.Equal(headers, m.Metadata.Headers)
	ver, err := semver.NewVersion(msgDataSchemaVersion)
	assertions.NoError(err)
	assertions.Equal(ver, m.DataSchemaVersion)
	assertions.Equal(msgDataType, m.Type)
}
