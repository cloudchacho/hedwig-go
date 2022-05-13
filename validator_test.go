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

func (f *fakeEncoder) IsBinary() bool {
	return true
}

type fakeDecoder struct {
	mock.Mock
}

func (f *fakeDecoder) DecodeMessageType(schema string) (string, *semver.Version, error) {
	args := f.Called(schema)
	return args.String(0), args.Get(1).(*semver.Version), args.Error(2)
}

func (f *fakeDecoder) ExtractData(messagePayload []byte, attributes map[string]string) (MetaAttributes, interface{}, error) {
	args := f.Called(messagePayload, attributes)
	return args.Get(0).(MetaAttributes), args.Get(1), args.Error(2)
}

func (f *fakeDecoder) DecodeData(messageType string, version *semver.Version, data interface{}) (interface{}, error) {
	args := f.Called(messageType, version, data)
	return args.Get(0), args.Error(1)
}

func (s *ValidatorTestSuite) TestSerialize() {
	s.encoder.On("VerifyKnownMinorVersion", s.message.Type, s.message.DataSchemaVersion).Return(nil)
	schema := "user-created/1.0"
	s.encoder.On("EncodeMessageType", s.message.Type, s.message.DataSchemaVersion).Return(schema)

	payload := []byte("user-created/1.0 C_123")

	s.encoder.On("EncodeData", s.message.Data, true, s.metaAttrs).
		Return(payload, nil)

	s.decoder.On("DecodeMessageType", schema).Return(s.message.Type, s.message.DataSchemaVersion, nil)
	s.decoder.On("DecodeData", s.message.Type, s.message.DataSchemaVersion, payload).
		Return(s.message.Data, nil)

	returnedPayload, returnedAttributes, err := s.validator.serialize(s.message, false)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), returnedPayload, payload)
	assert.Equal(s.T(), returnedAttributes, s.attributes)

	s.encoder.AssertExpectations(s.T())
	s.decoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestSerializeContainerized() {
	s.validator.withUseTransportMessageAttributes(false)

	s.encoder.On("VerifyKnownMinorVersion", s.message.Type, s.message.DataSchemaVersion).Return(nil)
	schema := "user-created/1.0"
	s.encoder.On("EncodeMessageType", s.message.Type, s.message.DataSchemaVersion).Return(schema)

	payload := []byte("user-created/1.0 C_123")
	data := map[string]string{"vehicle_id": "C_123"}

	s.encoder.On("EncodeData", s.message.Data, false, s.metaAttrs).
		Return(payload, nil)

	s.decoder.On("ExtractData", payload, s.metaAttrs.Headers).Return(s.metaAttrs, data, nil)
	s.decoder.On("DecodeMessageType", schema).Return(s.message.Type, s.message.DataSchemaVersion, nil)
	s.decoder.On("DecodeData", s.message.Type, s.message.DataSchemaVersion, data).
		Return(s.message.Data, nil)

	returnedPayload, returnedAttributes, err := s.validator.serialize(s.message, false)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), returnedPayload, payload)
	assert.Equal(s.T(), returnedAttributes, s.metaAttrs.Headers)

	s.encoder.AssertExpectations(s.T())
	s.decoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestSerializeUnknownMinor() {
	message, err := NewMessage("user-created", "1.0", nil, fakeHedwigDataField{VehicleID: "C_123"}, "myapp")
	require.NoError(s.T(), err)

	s.encoder.On("VerifyKnownMinorVersion", message.Type, message.DataSchemaVersion).Return(errors.New("unknown minor version"))

	_, _, err = s.validator.serialize(message, false)
	s.EqualError(err, "unknown minor version")

	s.encoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestSerializeEncodeFailed() {
	s.encoder.On("VerifyKnownMinorVersion", s.message.Type, s.message.DataSchemaVersion).Return(nil)
	schema := "user-created/1.0"
	s.encoder.On("EncodeMessageType", s.message.Type, s.message.DataSchemaVersion).Return(schema)

	s.encoder.On("EncodeData", s.message.Data, true, s.metaAttrs).
		Return([]byte(""), errors.New("can't serialize data"))

	_, _, err := s.validator.serialize(s.message, false)
	s.EqualError(err, "can't serialize data")

	s.encoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestSerializeValidationFailure() {
	s.encoder.On("VerifyKnownMinorVersion", s.message.Type, s.message.DataSchemaVersion).Return(nil)
	schema := "user-created/1.0"
	s.encoder.On("EncodeMessageType", s.message.Type, s.message.DataSchemaVersion).Return(schema)

	payload := []byte("user-created/1.0 C_123")

	s.encoder.On("EncodeData", s.message.Data, true, s.metaAttrs).
		Return(payload, nil)

	s.decoder.On("DecodeMessageType", schema).
		Return("", (*semver.Version)(nil), errors.New("invalid message type"))

	_, _, err := s.validator.serialize(s.message, false)
	s.EqualError(err, "invalid message type")

	s.encoder.AssertExpectations(s.T())
	s.decoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestDeserialize() {
	schema := "user-created/1.0"

	payload := []byte("user-created/1.0 C_123")

	s.decoder.On("DecodeMessageType", schema).Return(s.message.Type, s.message.DataSchemaVersion, nil)
	s.decoder.On("DecodeData", s.message.Type, s.message.DataSchemaVersion, payload).Return(s.message.Data, nil)

	returnedMessage, err := s.validator.deserialize(payload, s.attributes, nil, false)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), returnedMessage, s.message)

	s.decoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestDeserializeExtractionFailure() {
	payload := []byte("user-created/1.0 C_123")

	s.validator.withUseTransportMessageAttributes(false)

	s.decoder.On("ExtractData", payload, s.attributes).
		Return(MetaAttributes{}, nil, errors.New("invalid payload"))

	_, err := s.validator.deserialize(payload, s.attributes, nil, false)
	s.EqualError(err, "invalid payload")

	s.decoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestDeserializeUnknownMessageType() {
	payload := []byte("user-created/1.0 C_123")

	s.decoder.On("DecodeMessageType", s.metaAttrs.Schema).
		Return(s.message.Type, s.message.DataSchemaVersion, errors.New("invalid message type"))

	_, err := s.validator.deserialize(payload, s.attributes, nil, false)
	s.EqualError(err, "invalid message type")

	s.decoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestDeserializeUnknownFormatVersion() {
	s.attributes["hedwig_format_version"] = "1.1"

	payload := []byte("user-created/1.0 C_123")

	_, err := s.validator.deserialize(payload, s.attributes, nil, false)
	s.EqualError(err, "Invalid format version: 1.1")

	s.decoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestDeserializeInvalidData() {
	schema := "user-created/1.0"

	payload := []byte("user-created/1.0 C_123")

	s.decoder.On("DecodeMessageType", schema).Return(s.message.Type, s.message.DataSchemaVersion, nil)
	s.decoder.On("DecodeData", s.message.Type, s.message.DataSchemaVersion, payload).
		Return(s.message.Data, errors.New("invalid data"))

	_, err := s.validator.deserialize(payload, s.attributes, nil, false)
	s.EqualError(err, "invalid data")

	s.decoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestDeserializeInvalidHeaders() {
	s.validator.withUseTransportMessageAttributes(false)

	payload := []byte("user-created/1.0 C_123")
	data := map[string]string{"vehicle_id": "C_123"}

	s.metaAttrs.Headers["hedwig_id"] = "123"

	s.decoder.On("ExtractData", payload, s.attributes).Return(s.metaAttrs, data, nil)

	_, err := s.validator.deserialize(payload, s.attributes, nil, false)
	s.EqualError(err, "invalid header key: 'hedwig_id' - can't begin with reserved namespace 'hedwig_'")

	s.decoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestEncodeMetaAttrs() {
	metaAttrs := MetaAttributes{
		Timestamp:     time.Unix(1621550514, 123000000),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "123",
		Schema:        "https://github.com/cloudchacho/hedwig-go/schema#/schemas/vehicle_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	}
	attributes := s.validator.encodeMetaAttributes(metaAttrs)
	s.Equal(attributes, map[string]string{
		"hedwig_message_timestamp": "1621550514123",
		"hedwig_publisher":         "myapp",
		"foo":                      "bar",
		"hedwig_id":                "123",
		"hedwig_schema":            "https://github.com/cloudchacho/hedwig-go/schema#/schemas/vehicle_created/1.0",
		"hedwig_format_version":    "1.0",
	})

	metaAttrs.Headers = nil
	attributes = s.validator.encodeMetaAttributes(metaAttrs)
	s.Equal(attributes, map[string]string{
		"hedwig_message_timestamp": "1621550514123",
		"hedwig_publisher":         "myapp",
		"hedwig_id":                "123",
		"hedwig_schema":            "https://github.com/cloudchacho/hedwig-go/schema#/schemas/vehicle_created/1.0",
		"hedwig_format_version":    "1.0",
	})
}

func (s *ValidatorTestSuite) TestDecodeMetaAttrs() {
	attributes := map[string]string{
		"hedwig_message_timestamp": "1621550514123",
		"hedwig_publisher":         "myapp",
		"foo":                      "bar",
		"hedwig_id":                "123",
		"hedwig_schema":            "https://github.com/cloudchacho/hedwig-go/schema#/schemas/vehicle_created/1.0",
		"hedwig_format_version":    "1.0",
	}
	metaAttrs, err := s.validator.decodeMetaAttributes(attributes)
	s.NoError(err)
	s.Equal(metaAttrs, MetaAttributes{
		Timestamp:     time.Unix(1621550514, 123000000),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "123",
		Schema:        "https://github.com/cloudchacho/hedwig-go/schema#/schemas/vehicle_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	})
}

func (s *ValidatorTestSuite) TestDecodeMetaAttrsValidation() {
	attributes := map[string]string{
		"hedwig_message_timestamp": "1621550514123",
		"hedwig_publisher":         "myapp",
		"foo":                      "bar",
		"hedwig_id":                "123",
		"hedwig_schema":            "https://github.com/cloudchacho/hedwig-go/schema#/schemas/vehicle_created/1.0",
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

func (s *ValidatorTestSuite) TestSerializeFirehose() {
	s.encoder.On("VerifyKnownMinorVersion", s.message.Type, s.message.DataSchemaVersion).Return(nil)
	schema := "user-created/1.0"
	s.encoder.On("EncodeMessageType", s.message.Type, s.message.DataSchemaVersion).Return(schema)

	payload := []byte("user-created/1.0 C_123")

	s.encoder.On("EncodeData", s.message.Data, false, s.metaAttrs).
		Return(payload, nil)

	s.decoder.On("DecodeMessageType", schema).Return(s.message.Type, s.message.DataSchemaVersion, nil)
	s.decoder.On("DecodeData", s.message.Type, s.message.DataSchemaVersion, payload).
		Return(s.message.Data, nil)
	s.decoder.On("ExtractData", payload, map[string]string{"foo": "bar"}).Return(s.metaAttrs, payload, nil)
	s.decoder.On("ExtractData", payload, map[string]string{}).Return(s.metaAttrs, payload, nil)
	res, err := s.validator.SerializeFirehose(s.message)
	s.Nil(err)
	expected := []byte("\x16\x00\x00\x00\x00\x00\x00\x00user-created/1.0 C_123")
	s.Equal(res, expected)
}

func (s *ValidatorTestSuite) TestSerializeFirehoseError() {
	s.encoder.On("VerifyKnownMinorVersion", s.message.Type, s.message.DataSchemaVersion).Return(errors.New("bad version"))
	schema := "user-created/1.0"
	s.encoder.On("EncodeMessageType", s.message.Type, s.message.DataSchemaVersion).Return(schema)

	payload := []byte("user-created/1.0 C_123")

	s.encoder.On("EncodeData", s.message.Data, false, s.metaAttrs).
		Return(payload, nil)

	s.decoder.On("DecodeMessageType", schema).Return(s.message.Type, s.message.DataSchemaVersion, nil)
	s.decoder.On("DecodeData", s.message.Type, s.message.DataSchemaVersion, payload).
		Return(s.message.Data, nil)
	s.decoder.On("ExtractData", payload, map[string]string{"foo": "bar"}).Return(s.metaAttrs, payload, nil)
	s.decoder.On("ExtractData", payload, map[string]string{}).Return(s.metaAttrs, payload, nil)
	_, err := s.validator.SerializeFirehose(s.message)
	s.EqualError(err, "bad version")
}

func (s *ValidatorTestSuite) TestDeSerializeFirehose() {
	// first 8 bytes is length of message (22) in this case
	payload := []byte("\x16\x00\x00\x00\x00\x00\x00\x00user-created/1.0 C_123")
	schema := "user-created/1.0"

	s.encoder.On("VerifyKnownMinorVersion", s.message.Type, s.message.DataSchemaVersion).Return(nil)
	s.encoder.On("EncodeMessageType", s.message.Type, s.message.DataSchemaVersion).Return(schema)
	s.encoder.On("EncodeData", s.message.Data, true, s.metaAttrs).
		Return(payload, nil)

	s.decoder.On("ExtractData", payload[8:], map[string]string{}).Return(s.metaAttrs, payload, nil)
	s.decoder.On("DecodeMessageType", schema).Return(s.message.Type, s.message.DataSchemaVersion, nil)
	s.decoder.On("DecodeData", s.message.Type, s.message.DataSchemaVersion, payload).
		Return(s.message.Data, nil)
	line, _, err := s.validator.serialize(s.message, true)
	s.Nil(err)
	res, err := s.validator.DeserializeFirehose(line)
	s.Nil(err)
	s.Equal(res, s.message)

}

func (s *ValidatorTestSuite) TestNew() {
	assert.NotNil(s.T(), s.validator)
}

type ValidatorTestSuite struct {
	suite.Suite
	validator  *messageValidator
	backend    *fakeBackend
	encoder    *fakeEncoder
	decoder    *fakeDecoder
	attributes map[string]string
	message    *Message
	metaAttrs  MetaAttributes
}

func (s *ValidatorTestSuite) SetupTest() {
	backend := &fakeBackend{}
	encoder := &fakeEncoder{}
	decoder := &fakeDecoder{}
	message, err := NewMessage(
		"user-created", "1.0", map[string]string{"foo": "bar"}, fakeHedwigDataField{VehicleID: "C_123"}, "myapp")
	require.NoError(s.T(), err)
	// fixed timestamp
	message.Metadata.Timestamp = time.Unix(1621550514, 123000000)
	schema := "user-created/1.0"

	s.validator = newMessageValidator(encoder, decoder)
	s.encoder = encoder
	s.decoder = decoder
	s.backend = backend
	s.attributes = map[string]string{
		"hedwig_message_timestamp": "1621550514123",
		"hedwig_publisher":         "myapp",
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
