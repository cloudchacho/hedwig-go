package hedwig

import (
	"fmt"
	"strconv"
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
	// EncodeData encodes the message with appropriate format for transport over the wire
	EncodeData(data interface{}, useMessageTransport bool, metaAttrs MetaAttributes) ([]byte, error)

	// DecodeData validates and decodes data
	DecodeData(messageType string, version *semver.Version, data interface{}) (interface{}, error)

	// VerifyKnownMinorVersion checks that message version is known to us
	VerifyKnownMinorVersion(messageType string, version *semver.Version) error

	// EncodeMessageType encodes the message type with appropriate format for transport over the wire
	EncodeMessageType(messageType string, version *semver.Version) string

	// DecodeMessageType decodes message type from meta attributes
	DecodeMessageType(schema string) (string, *semver.Version, error)

	// ExtractData extracts data from the on-the-wire payload when not using message transport
	ExtractData(messagePayload []byte, attributes map[string]string) (MetaAttributes, interface{}, error)
}

type messageValidator struct {
	encoder              IEncoder
	currentFormatVersion *semver.Version
	settings             *Settings
}

// decodeMetaAttributes decodes message transport attributes as MetaAttributes
func (v *messageValidator) decodeMetaAttributes(attributes map[string]string) (MetaAttributes, error) {
	metaAttrs := MetaAttributes{}
	if value, ok := attributes["hedwig_format_version"]; !ok {
		return metaAttrs, errors.New("value not found for attribute: 'hedwig_format_version'")
	} else {
		if version, err := semver.NewVersion(value); err != nil {
			return metaAttrs, errors.Errorf("invalid value '%s' found for attribute: 'hedwig_format_version'", value)
		} else {
			metaAttrs.FormatVersion = version
		}
	}
	if value, ok := attributes["hedwig_id"]; !ok {
		return metaAttrs, errors.New("value not found for attribute: 'hedwig_id'")
	} else {
		metaAttrs.ID = value
	}
	if value, ok := attributes["hedwig_message_timestamp"]; !ok {
		return metaAttrs, errors.New("value not found for attribute: 'hedwig_id'")
	} else {
		if timestamp, err := strconv.ParseInt(value, 10, 64); err != nil {
			return metaAttrs, errors.Errorf("invalid value '%s' found for attribute: 'hedwig_message_timestamp'", value)
		} else {
			unixTime := time.Unix(0, timestamp*int64(time.Millisecond))
			metaAttrs.Timestamp = unixTime
		}
	}
	if value, ok := attributes["hedwig_publisher"]; !ok {
		return metaAttrs, errors.New("value not found for attribute: 'hedwig_publisher'")
	} else {
		metaAttrs.Publisher = value
	}
	if value, ok := attributes["hedwig_schema"]; !ok {
		return metaAttrs, errors.New("value not found for attribute: 'hedwig_schema'")
	} else {
		metaAttrs.Schema = value
	}

	if len(attributes) != 0 {
		metaAttrs.Headers = map[string]string{}
	}
	for k, v := range attributes {
		if !strings.HasPrefix(k, "hedwig_") {
			metaAttrs.Headers[k] = v
		}
	}
	return metaAttrs, nil
}

// encodeMetaAttributes decodes MetaAttributes as message transport attributes
func (v *messageValidator) encodeMetaAttributes(metaAttrs MetaAttributes) map[string]string {
	attributes := map[string]string{
		"hedwig_format_version":    fmt.Sprintf("%d.%d", metaAttrs.FormatVersion.Major(), metaAttrs.FormatVersion.Minor()),
		"hedwig_id":                metaAttrs.ID,
		"hedwig_message_timestamp": fmt.Sprintf("%d", (metaAttrs.Timestamp.UnixNano() / int64(time.Millisecond))),
		"hedwig_publisher":         metaAttrs.Publisher,
		"hedwig_schema":            metaAttrs.Schema,
	}
	for k, v := range metaAttrs.Headers {
		attributes[k] = v
	}
	return attributes
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
	messagePayload, err := v.encoder.EncodeData(message.Data, *v.settings.UseTransportMessageAttributes, metaAttrs)
	if err != nil {
		return nil, nil, err
	}
	var attributes map[string]string
	if *v.settings.UseTransportMessageAttributes {
		attributes = v.encodeMetaAttributes(metaAttrs)
	} else {
		attributes = message.Metadata.Headers
	}
	// validate payload from scratch before publishing
	_, err = v.Deserialize(messagePayload, attributes, nil)
	if err != nil {
		return nil, nil, err
	}
	return messagePayload, attributes, nil
}

func (v *messageValidator) Deserialize(messagePayload []byte, attributes map[string]string, providerMetadata interface{}) (*Message, error) {
	var metaAttrs MetaAttributes
	var data interface{}
	var err error
	if *v.settings.UseTransportMessageAttributes {
		metaAttrs, err = v.decodeMetaAttributes(attributes)
		if err != nil {
			return nil, err
		}
		data = messagePayload
	} else {
		metaAttrs, data, err = v.encoder.ExtractData(messagePayload, attributes)
	}
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
	data, err = v.encoder.DecodeData(messageType, version, data)
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
