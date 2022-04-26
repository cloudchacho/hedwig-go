package hedwig

import (
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/Masterminds/semver"
)

type MetaAttributes struct {
	Timestamp     time.Time
	Publisher     string
	Headers       map[string]string
	ID            string
	Schema        string
	FormatVersion *semver.Version
}

type messageValidator struct {
	encoder                       Encoder
	decoder                       Decoder
	currentFormatVersion          *semver.Version
	useTransportMessageAttributes bool
}

func (v *messageValidator) getPayloadandAttributes(message *Message) ([]byte, map[string]string, error) {
	err := v.encoder.VerifyKnownMinorVersion(message.Type, message.DataSchemaVersion)
	if err != nil {
		return nil, nil, err
	}
	err = v.verifyHeaders(message.Metadata.Headers)
	if err != nil {
		return nil, nil, err
	}
	schema := v.encoder.EncodeMessageType(message.Type, message.DataSchemaVersion)
	metaAttrs := MetaAttributes{
		message.Metadata.Timestamp,
		message.Metadata.Publisher,
		message.Metadata.Headers,
		message.ID,
		schema,
		v.currentFormatVersion,
	}
	messagePayload, err := v.encoder.EncodeData(message.Data, v.useTransportMessageAttributes, metaAttrs)
	if err != nil {
		return nil, nil, err
	}
	var attributes map[string]string
	if v.useTransportMessageAttributes {
		attributes = v.encodeMetaAttributes(metaAttrs)
	} else {
		attributes = message.Metadata.Headers
	}
	return messagePayload, attributes, nil
}

func (v *messageValidator) DeserializeFirehose(line []byte) (*Message, error) {
	// defer reset of useTransportMessageAttributes
	defer v.withUseTransportMessageAttributes(v.useTransportMessageAttributes)
	v.withUseTransportMessageAttributes(false)
	var messagePayload []byte
	if v.encoder.IsBinary() {
		// TLV format: 8 bytes for size of message, n bytes for the actual message
		copy(messagePayload, line[8:])
	} else {
		// last char is new line, skip that
		copy(messagePayload, line[:len(line)-1])
	}
	return v.deserialize(messagePayload, map[string]string{}, nil)
}

func (v *messageValidator) SerializeFirehose(message *Message) ([]byte, error) {
	// defer reset of useTransportMessageAttributes
	defer v.withUseTransportMessageAttributes(v.useTransportMessageAttributes)
	v.withUseTransportMessageAttributes(false)
	messagePayload, _, err := v.serialize(message)
	if err != nil {
		return nil, err
	}
	var encodedMessage []byte
	if v.encoder.IsBinary() {
		// TLV format: 8 bytes for size of message, n bytes for the actual message
		encodedMessage = make([]byte, 8+len(messagePayload))
		binary.LittleEndian.PutUint64(encodedMessage, uint64(len(messagePayload)))
		copy(encodedMessage[8:], messagePayload)
	} else {
		encodedMessage = make([]byte, 1+len(messagePayload))
		copy(encodedMessage, messagePayload)
		encodedMessage[len(encodedMessage)-1] = byte('\n')
	}
	_, err = v.DeserializeFirehose(encodedMessage)
	return encodedMessage, err
}

// decodeMetaAttributes decodes message transport attributes as MetaAttributes
func (v *messageValidator) decodeMetaAttributes(attributes map[string]string) (MetaAttributes, error) {
	metaAttrs := MetaAttributes{}
	var value string
	var ok bool
	if value, ok = attributes["hedwig_format_version"]; !ok {
		return metaAttrs, errors.New("value not found for attribute: 'hedwig_format_version'")
	}
	var version *semver.Version
	var err error
	if version, err = semver.NewVersion(value); err != nil {
		return metaAttrs, errors.Errorf("invalid value '%s' found for attribute: 'hedwig_format_version'", value)
	}
	metaAttrs.FormatVersion = version
	if value, ok = attributes["hedwig_id"]; !ok {
		return metaAttrs, errors.New("value not found for attribute: 'hedwig_id'")
	}
	metaAttrs.ID = value
	if value, ok = attributes["hedwig_message_timestamp"]; !ok {
		return metaAttrs, errors.New("value not found for attribute: 'hedwig_id'")
	}
	var timestamp int64
	if timestamp, err = strconv.ParseInt(value, 10, 64); err != nil {
		return metaAttrs, errors.Errorf("invalid value '%s' found for attribute: 'hedwig_message_timestamp'", value)
	}
	unixTime := time.Unix(0, timestamp*int64(time.Millisecond))
	metaAttrs.Timestamp = unixTime
	if value, ok = attributes["hedwig_publisher"]; !ok {
		return metaAttrs, errors.New("value not found for attribute: 'hedwig_publisher'")
	}
	metaAttrs.Publisher = value
	if value, ok = attributes["hedwig_schema"]; !ok {
		return metaAttrs, errors.New("value not found for attribute: 'hedwig_schema'")
	}
	metaAttrs.Schema = value

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

func (v *messageValidator) serialize(message *Message) ([]byte, map[string]string, error) {
	messagePayload, attributes, err := v.getPayloadandAttributes(message)
	if err != nil {
		return nil, nil, err
	}
	// validate payload from scratch before publishing
	_, err = v.deserialize(messagePayload, attributes, nil)
	if err != nil {
		return nil, nil, err
	}
	return messagePayload, attributes, nil
}

func (v *messageValidator) deserialize(messagePayload []byte, attributes map[string]string, providerMetadata interface{}) (*Message, error) {
	var metaAttrs MetaAttributes
	var data interface{}
	var err error
	if v.useTransportMessageAttributes {
		metaAttrs, err = v.decodeMetaAttributes(attributes)
		if err != nil {
			return nil, err
		}
		data = messagePayload
	} else {
		metaAttrs, data, err = v.decoder.ExtractData(messagePayload, attributes)
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
	messageType, version, err := v.decoder.DecodeMessageType(metaAttrs.Schema)
	if err != nil {
		return nil, err
	}
	data, err = v.decoder.DecodeData(messageType, version, data)
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

func (v *messageValidator) withUseTransportMessageAttributes(useTransportMessageAttributes bool) {
	v.useTransportMessageAttributes = useTransportMessageAttributes
}

func newMessageValidator(encoder Encoder, decoder Decoder) *messageValidator {
	v := &messageValidator{
		encoder:              encoder,
		decoder:              decoder,
		currentFormatVersion: semver.MustParse("1.0"),
	}
	v.useTransportMessageAttributes = true
	return v
}
