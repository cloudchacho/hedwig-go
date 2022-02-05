package hedwig

import (
	"time"

	"github.com/Masterminds/semver"
	"github.com/pkg/errors"
	uuid "github.com/satori/go.uuid"
)

type metadata struct {
	// Headers are arbitrary maps attached to the message. This is the equivalent of HTTP Headers.
	Headers map[string]string

	// Publisher name that published this message
	Publisher string

	// Timestamp when this message was created
	Timestamp time.Time

	// ProviderMetadata represents backend provider specific metadata, e.g. AWS receipt, or Pub/Sub ack ID
	// For concrete type of metadata, check the documentation of your backend class
	ProviderMetadata interface{}
}

// Message model for hedwig messages.
type Message struct {
	Data              interface{}
	Type              string
	DataSchemaVersion *semver.Version
	ID                string
	Metadata          metadata
}

func createMetadata(settings *Settings, headers map[string]string) metadata {
	return metadata{
		Headers:   headers,
		Publisher: settings.PublisherName,
		Timestamp: time.Now(),
	}
}

// newMessageWithID creates new Hedwig messages
func newMessageWithID(
	settings *Settings, id string, dataType string, dataSchemaVersion string,
	metadata metadata, data interface{}) (*Message, error) {
	if data == nil {
		return nil, errors.New("expected non-nil data")
	}

	version, err := semver.NewVersion(dataSchemaVersion)
	if err != nil {
		return nil, errors.Errorf("invalid version: %s", dataSchemaVersion)
	}

	m := &Message{
		Data:              data,
		Type:              dataType,
		DataSchemaVersion: version,
		ID:                id,
		Metadata:          metadata,
	}
	return m, nil
}

// NewMessage creates new Hedwig messages based off of message type and Schema version
func NewMessage(settings *Settings, dataType string, dataSchemaVersion string, headers map[string]string, data interface{}) (*Message, error) {
	// Generate uuid for ID
	msgUUID := uuid.NewV4()
	msgID := msgUUID.String()

	if headers == nil {
		headers = make(map[string]string)
	}

	return newMessageWithID(settings, msgID, dataType, dataSchemaVersion, createMetadata(settings, headers), data)
}
