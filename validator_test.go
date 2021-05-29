/*
 * Author: Michael Ngo
 */

package hedwig

import (
	"testing"
	"time"

	"github.com/Masterminds/semver"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

type fakeValidator struct {
	mock.Mock
}

func (f *fakeValidator) Serialize(message *Message) ([]byte, map[string]string, error) {
	args := f.Called(message)
	return args.Get(0).([]byte), args.Get(1).(map[string]string), args.Error(2)
}

func (f *fakeValidator) Deserialize(messagePayload []byte, attributes map[string]string, providerMetadata interface{}) (*Message, error) {
	args := f.Called(messagePayload, attributes, providerMetadata)
	return args.Get(0).(*Message), args.Error(1)
}

type fakeEncoder struct {
	mock.Mock
}

func (f *fakeEncoder) EncodeData(data interface{}, useMessageTransport bool, metaAttrs MetaAttributes) ([]byte, error) {
	args := f.Called(data, useMessageTransport, metaAttrs)
	return args.Get(0).([]byte), args.Error(1)
}

func (f *fakeEncoder) VerifyKnownMinorVersion(messageType string, version *semver.Version) error {
	args := f.Called(messageType, version)
	return args.Error(0)
}

func (f *fakeEncoder) EncodeMessageType(messageType string, version *semver.Version) string {
	args := f.Called(messageType, version)
	return args.String(0)
}

func (f *fakeEncoder) DecodeMessageType(schema string) (string, *semver.Version, error) {
	args := f.Called(schema)
	return args.String(0), args.Get(1).(*semver.Version), args.Error(2)
}

func (f *fakeEncoder) ExtractData(messagePayload []byte, attributes map[string]string) (MetaAttributes, interface{}, error) {
	args := f.Called(messagePayload, attributes)
	return args.Get(0).(MetaAttributes), args.Get(1), args.Error(2)
}

func (f *fakeEncoder) DecodeData(messageType string, version *semver.Version, data interface{}) (interface{}, error) {
	args := f.Called(messageType, version, data)
	return args.Get(0), args.Error(1)
}

func (s *ValidatorTestSuite) TestSerialize() {
	s.encoder.On("VerifyKnownMinorVersion", s.message.Type, s.message.DataSchemaVersion).Return(nil)
	schema := "user-created/1.0"
	s.encoder.On("EncodeMessageType", s.message.Type, s.message.DataSchemaVersion).Return(schema)

	payload := []byte("user-created/1.0 C_123")

	s.encoder.On("EncodeData", s.message.Data, *s.settings.UseTransportMessageAttributes, s.metaAttrs).
		Return(payload, nil)

	s.encoder.On("DecodeMessageType", schema).Return(s.message.Type, s.message.DataSchemaVersion, nil)
	s.encoder.On("DecodeData", s.message.Type, s.message.DataSchemaVersion, payload).
		Return(s.message.Data, nil)

	returnedPayload, returnedAttributes, err := s.validator.Serialize(s.message)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), returnedPayload, payload)
	assert.Equal(s.T(), returnedAttributes, s.attributes)

	s.encoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestSerializeContainerized() {
	*s.settings.UseTransportMessageAttributes = false

	s.encoder.On("VerifyKnownMinorVersion", s.message.Type, s.message.DataSchemaVersion).Return(nil)
	schema := "user-created/1.0"
	s.encoder.On("EncodeMessageType", s.message.Type, s.message.DataSchemaVersion).Return(schema)

	payload := []byte("user-created/1.0 C_123")
	data := map[string]string{"vehicle_id": "C_123"}

	s.encoder.On("EncodeData", s.message.Data, *s.settings.UseTransportMessageAttributes, s.metaAttrs).
		Return(payload, nil)

	s.encoder.On("ExtractData", payload, s.metaAttrs.Headers).Return(s.metaAttrs, data, nil)
	s.encoder.On("DecodeMessageType", schema).Return(s.message.Type, s.message.DataSchemaVersion, nil)
	s.encoder.On("DecodeData", s.message.Type, s.message.DataSchemaVersion, data).
		Return(s.message.Data, nil)

	returnedPayload, returnedAttributes, err := s.validator.Serialize(s.message)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), returnedPayload, payload)
	assert.Equal(s.T(), returnedAttributes, s.metaAttrs.Headers)

	s.encoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestSerializeUnknownMinor() {
	message, err := NewMessage(s.settings, "user-created", "1.0", nil, fakeHedwigDataField{VehicleID: "C_123"})
	require.NoError(s.T(), err)

	s.encoder.On("VerifyKnownMinorVersion", message.Type, message.DataSchemaVersion).Return(errors.New("unknown minor version"))

	_, _, err = s.validator.Serialize(message)
	s.EqualError(err, "unknown minor version")

	s.encoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestSerializeEncodeFailed() {
	s.encoder.On("VerifyKnownMinorVersion", s.message.Type, s.message.DataSchemaVersion).Return(nil)
	schema := "user-created/1.0"
	s.encoder.On("EncodeMessageType", s.message.Type, s.message.DataSchemaVersion).Return(schema)

	s.encoder.On("EncodeData", s.message.Data, *s.settings.UseTransportMessageAttributes, s.metaAttrs).
		Return([]byte(""), errors.New("can't serialize data"))

	_, _, err := s.validator.Serialize(s.message)
	s.EqualError(err, "can't serialize data")

	s.encoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestSerializeValidationFailure() {
	s.encoder.On("VerifyKnownMinorVersion", s.message.Type, s.message.DataSchemaVersion).Return(nil)
	schema := "user-created/1.0"
	s.encoder.On("EncodeMessageType", s.message.Type, s.message.DataSchemaVersion).Return(schema)

	payload := []byte("user-created/1.0 C_123")

	s.encoder.On("EncodeData", s.message.Data, *s.settings.UseTransportMessageAttributes, s.metaAttrs).
		Return(payload, nil)

	s.encoder.On("DecodeMessageType", schema).
		Return("", (*semver.Version)(nil), errors.New("invalid message type"))

	_, _, err := s.validator.Serialize(s.message)
	s.EqualError(err, "invalid message type")

	s.encoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestDeserialize() {
	schema := "user-created/1.0"

	payload := []byte("user-created/1.0 C_123")

	s.encoder.On("DecodeMessageType", schema).Return(s.message.Type, s.message.DataSchemaVersion, nil)
	s.encoder.On("DecodeData", s.message.Type, s.message.DataSchemaVersion, payload).Return(s.message.Data, nil)

	returnedMessage, err := s.validator.Deserialize(payload, s.attributes, nil)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), returnedMessage, s.message)

	s.encoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestDeserializeExtractionFailure() {
	payload := []byte("user-created/1.0 C_123")

	*s.settings.UseTransportMessageAttributes = false

	s.encoder.On("ExtractData", payload, s.attributes).
		Return(MetaAttributes{}, nil, errors.New("invalid payload"))

	_, err := s.validator.Deserialize(payload, s.attributes, nil)
	s.EqualError(err, "invalid payload")

	s.encoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestDeserializeUnknownMessageType() {
	payload := []byte("user-created/1.0 C_123")

	s.encoder.On("DecodeMessageType", s.metaAttrs.Schema).
		Return(s.message.Type, s.message.DataSchemaVersion, errors.New("invalid message type"))

	_, err := s.validator.Deserialize(payload, s.attributes, nil)
	s.EqualError(err, "invalid message type")

	s.encoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestDeserializeUnknownFormatVersion() {
	s.attributes["hedwig_format_version"] = "1.1"

	payload := []byte("user-created/1.0 C_123")

	_, err := s.validator.Deserialize(payload, s.attributes, nil)
	s.EqualError(err, "Invalid format version: 1.1")

	s.encoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestDeserializeInvalidData() {
	schema := "user-created/1.0"

	payload := []byte("user-created/1.0 C_123")

	s.encoder.On("DecodeMessageType", schema).Return(s.message.Type, s.message.DataSchemaVersion, nil)
	s.encoder.On("DecodeData", s.message.Type, s.message.DataSchemaVersion, payload).
		Return(s.message.Data, errors.New("invalid data"))

	_, err := s.validator.Deserialize(payload, s.attributes, nil)
	s.EqualError(err, "invalid data")

	s.encoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestDeserializeInvalidHeaders() {
	*s.settings.UseTransportMessageAttributes = false

	payload := []byte("user-created/1.0 C_123")
	data := map[string]string{"vehicle_id": "C_123"}

	s.metaAttrs.Headers["hedwig_id"] = "123"

	s.encoder.On("ExtractData", payload, s.attributes).Return(s.metaAttrs, data, nil)

	_, err := s.validator.Deserialize(payload, s.attributes, nil)
	s.EqualError(err, "invalid header key: 'hedwig_id' - can't begin with reserved namespace 'hedwig_'")

	s.encoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestEncodeMetaAttrs() {
	metaAttrs := MetaAttributes{
		Timestamp:     time.Unix(1621550514, 123000000),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "123",
		Schema:        "https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	}
	attributes := s.validator.encodeMetaAttributes(metaAttrs)
	s.Equal(attributes, map[string]string{
		"hedwig_message_timestamp": "1621550514123",
		"hedwig_publisher":         "myapp",
		"foo":                      "bar",
		"hedwig_id":                "123",
		"hedwig_schema":            "https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0",
		"hedwig_format_version":    "1.0",
	})

	metaAttrs.Headers = nil
	attributes = s.validator.encodeMetaAttributes(metaAttrs)
	s.Equal(attributes, map[string]string{
		"hedwig_message_timestamp": "1621550514123",
		"hedwig_publisher":         "myapp",
		"hedwig_id":                "123",
		"hedwig_schema":            "https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0",
		"hedwig_format_version":    "1.0",
	})
}

func (s *ValidatorTestSuite) TestDecodeMetaAttrs() {
	attributes := map[string]string{
		"hedwig_message_timestamp": "1621550514123",
		"hedwig_publisher":         "myapp",
		"foo":                      "bar",
		"hedwig_id":                "123",
		"hedwig_schema":            "https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0",
		"hedwig_format_version":    "1.0",
	}
	metaAttrs, err := s.validator.decodeMetaAttributes(attributes)
	s.NoError(err)
	s.Equal(metaAttrs, MetaAttributes{
		Timestamp:     time.Unix(1621550514, 123000000),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "123",
		Schema:        "https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	})
}

func (s *ValidatorTestSuite) TestDecodeMetaAttrsValidation() {
	attributes := map[string]string{
		"hedwig_message_timestamp": "1621550514123",
		"hedwig_publisher":         "myapp",
		"foo":                      "bar",
		"hedwig_id":                "123",
		"hedwig_schema":            "https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0",
		"hedwig_format_version":    "1.0",
	}
	tests := map[string]struct {
		field    string
		badValue string
	}{
		"timestamp_missing":         {"hedwig_message_timestamp", "missing"},
		"timestamp_string":          {"hedwig_message_timestamp", "foobar"},
		"timestamp_iso_string":      {"hedwig_message_timestamp", "2021-05-20T16:02:41-0700"},
		"publisher_missing":         {"hedwig_publisher", "missing"},
		"id_missing":                {"hedwig_id", "missing"},
		"schema_missing":            {"hedwig_schema", "missing"},
		"format_version_missing":    {"hedwig_format_version", "missing"},
		"format_version_not_semver": {"hedwig_format_version", "foobar"},
	}

	for name, test := range tests {
		s.Run(name, func() {
			badAttributes := map[string]string{}
			for k, v := range attributes {
				if k == test.field {
					if test.badValue != "missing" {
						badAttributes[k] = test.badValue
					}
				} else {
					badAttributes[k] = v
				}
			}
			_, err := s.validator.decodeMetaAttributes(badAttributes)
			s.Error(err)
		})
	}
}

func (s *ValidatorTestSuite) TestNew() {
	assert.NotNil(s.T(), s.validator)
}

type ValidatorTestSuite struct {
	suite.Suite
	validator  *messageValidator
	backend    *fakeBackend
	settings   *Settings
	encoder    *fakeEncoder
	attributes map[string]string
	message    *Message
	metaAttrs  MetaAttributes
}

func (s *ValidatorTestSuite) SetupTest() {
	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		QueueName:    "dev-myapp",
		MessageRouting: map[MessageTypeMajorVersion]string{
			{
				MessageType:  "user-created",
				MajorVersion: 1,
			}: "dev-user-created-v1",
		},
		PublisherName: "myapp",
	}
	backend := &fakeBackend{}
	encoder := &fakeEncoder{}
	message, err := NewMessage(
		settings, "user-created", "1.0", map[string]string{"foo": "bar"}, fakeHedwigDataField{VehicleID: "C_123"})
	require.NoError(s.T(), err)
	// fixed timestamp
	message.Metadata.Timestamp = time.Unix(1621550514, 123000000)
	schema := "user-created/1.0"

	s.validator = NewMessageValidator(settings, encoder).(*messageValidator)
	s.encoder = encoder
	s.backend = backend
	s.settings = settings
	s.attributes = map[string]string{
		"hedwig_message_timestamp": "1621550514123",
		"hedwig_publisher":         settings.PublisherName,
		"foo":                      "bar",
		"hedwig_id":                message.ID,
		"hedwig_schema":            schema,
		"hedwig_format_version":    "1.0",
	}
	s.metaAttrs = MetaAttributes{
		message.Metadata.Timestamp,
		message.Metadata.Publisher,
		message.Metadata.Headers,
		message.ID,
		schema,
		semver.MustParse("1.0"),
	}

	s.message = message
}

func TestValidatorTestSuite(t *testing.T) {
	suite.Run(t, &ValidatorTestSuite{})
}
