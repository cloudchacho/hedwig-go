package hedwig

import (
	"fmt"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/Masterminds/semver"
)

// IMessageValidator handles validating Hedwig messages
type IMessageValidator interface {
	// Serialize the message for appropriate format for transport over the wire
	Serialize(message *Message) ([]byte, map[string]string, error)

	// Deserialize the message from the format over the wire
	Deserialize(messagePayload []byte, attributes map[string]string, providerMetadata interface{}) (*Message, error)
}

type MetaAttributes struct {
	Timestamp     time.Time
	Publisher     string
	Headers       map[string]string
	ID            string
	Schema        string
	FormatVersion *semver.Version
}

// IEncoder is responsible for encoding the message payload in appropriate format for over the wire transport
type IEncoder interface {
	// EncodePayload encodes the message with appropriate format for transport over the wire
	EncodePayload(data interface{}, useMessageTransport bool, metaAttrs MetaAttributes) ([]byte, map[string]string, error)

	// VerifyKnownMinorVersion checks that message version is known to us
	VerifyKnownMinorVersion(messageType string, version *semver.Version) error

	// EncodeMessageType encodes the message type with appropriate format for transport over the wire
	EncodeMessageType(messageType string, version *semver.Version) string

	// DecodeMessageType decodes message type from meta attributes
	DecodeMessageType(schema string) (string, *semver.Version, error)

	// ExtractData extracts data from the on-the-wire payload
	ExtractData(messagePayload []byte, attributes map[string]string, useMessageTransport bool) (MetaAttributes, interface{}, error)

	// DecodeData validates and decodes data
	DecodeData(metaAttrs MetaAttributes, messageType string, version *semver.Version, data interface{}) (interface{}, error)
}

type messageValidator struct {
	encoder              IEncoder
	currentFormatVersion *semver.Version
	settings             *Settings
}

func (v *messageValidator) Serialize(message *Message) ([]byte, map[string]string, error) {
	err := v.encoder.VerifyKnownMinorVersion(message.Type, message.DataSchemaVersion)
	if err != nil {
		return nil, nil, err
	}
	v.verifyHeaders(message.Metadata.Headers)
	schema := v.encoder.EncodeMessageType(message.Type, message.DataSchemaVersion)
	metaAttrs := MetaAttributes{
		message.Metadata.Timestamp,
		message.Metadata.Publisher,
		message.Metadata.Headers,
		message.ID,
		schema,
		v.currentFormatVersion,
	}
	messagePayload, msgAttrs, err := v.encoder.EncodePayload(message.Data, *v.settings.UseTransportMessageAttributes, metaAttrs)
	if err != nil {
		return nil, nil, err
	}
	// validate payload from scratch before publishing
	_, err = v.Deserialize(messagePayload, msgAttrs, nil)
	if err != nil {
		return nil, nil, err
	}
	return messagePayload, msgAttrs, nil
}

func (v *messageValidator) Deserialize(messagePayload []byte, attributes map[string]string, providerMetadata interface{}) (*Message, error) {
	metaAttrs, extractedData, err := v.encoder.ExtractData(messagePayload, attributes, *v.settings.UseTransportMessageAttributes)
	if err != nil {
		return nil, err
	}
	if !metaAttrs.FormatVersion.Equal(v.currentFormatVersion) {
		return nil, errors.Errorf("Invalid format version: %d.%d", metaAttrs.FormatVersion.Major(), metaAttrs.FormatVersion.Minor())
	}
	err = v.verifyHeaders(metaAttrs.Headers)
	if err != nil {
		return nil, err
	}
	messageType, version, err := v.encoder.DecodeMessageType(metaAttrs.Schema)
	if err != nil {
		return nil, err
	}
	data, err := v.encoder.DecodeData(metaAttrs, messageType, version, extractedData)
	if err != nil {
		return nil, err
	}
	return &Message{
		ID: metaAttrs.ID,
		Metadata: metadata{
			Timestamp:        metaAttrs.Timestamp,
			Headers:          metaAttrs.Headers,
			Publisher:        metaAttrs.Publisher,
			ProviderMetadata: providerMetadata,
		},
		Data:              data,
		Type:              messageType,
		DataSchemaVersion: version,
	}, nil
}

func (v *messageValidator) verifyHeaders(headers map[string]string) error {
	for k := range headers {
		if strings.HasPrefix(k, "hedwig_") {
			return fmt.Errorf("invalid header key: '%s' - can't begin with reserved namespace 'hedwig_'", k)
		}
	}
	return nil
}

func NewMessageValidator(settings *Settings, encoder IEncoder) IMessageValidator {
	settings.initDefaults()
	return &messageValidator{
		encoder:              encoder,
		currentFormatVersion: semver.MustParse("1.0"),
		settings:             settings,
	}
}
