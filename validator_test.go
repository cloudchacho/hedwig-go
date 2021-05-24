/*
 * Author: Michael Ngo
 */

package hedwig

import (
	"testing"

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

func (f *fakeEncoder) EncodePayload(data interface{}, useMessageTransport bool, metaAttrs MetaAttributes) ([]byte, map[string]string, error) {
	args := f.Called(data, useMessageTransport, metaAttrs)
	return args.Get(0).([]byte), args.Get(1).(map[string]string), args.Error(2)
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

func (f *fakeEncoder) ExtractData(messagePayload []byte, attributes map[string]string, useMessageTransport bool) (MetaAttributes, interface{}, error) {
	args := f.Called(messagePayload, attributes, useMessageTransport)
	return args.Get(0).(MetaAttributes), args.Get(1), args.Error(2)
}

func (f fakeEncoder) DecodeData(metaAttrs MetaAttributes, messageType string, version *semver.Version, data interface{}) (interface{}, error) {
	args := f.Called(metaAttrs, messageType, version, data)
	return args.Get(0), args.Error(1)
}

func (s *ValidatorTestSuite) TestSerialize() {
	message, err := NewMessage(s.settings, "user-created", "1.0", nil, fakeHedwigDataField{VehicleID: "C_123"})
	require.NoError(s.T(), err)

	s.encoder.On("VerifyKnownMinorVersion", message.Type, message.DataSchemaVersion).Return(nil)
	schema := "user-created/1.0"
	s.encoder.On("EncodeMessageType", message.Type, message.DataSchemaVersion).Return(schema)

	metaAttrs := MetaAttributes{
		message.Metadata.Timestamp,
		message.Metadata.Publisher,
		message.Metadata.Headers,
		message.ID,
		schema,
		semver.MustParse("1.0"),
	}

	payload := []byte("user-created/1.0 C_123")
	attributes := map[string]string{"hedwig_id": "123"}
	data := map[string]string{"vehicle_id": "C_123"}

	s.encoder.On("EncodePayload", message.Data, *s.settings.UseTransportMessageAttributes, metaAttrs).Return(payload, attributes, nil)

	s.encoder.On("ExtractData", payload, attributes, *s.settings.UseTransportMessageAttributes).Return(metaAttrs, data, nil)
	s.encoder.On("DecodeMessageType", schema).Return(message.Type, message.DataSchemaVersion, nil)
	s.encoder.On("DecodeData", metaAttrs, message.Type, message.DataSchemaVersion, data).Return(message.Data, nil)

	returnedPayload, returnedAttributes, err := s.validator.Serialize(message)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), returnedPayload, payload)
	assert.Equal(s.T(), returnedAttributes, attributes)

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
	message, err := NewMessage(s.settings, "user-created", "1.0", nil, fakeHedwigDataField{VehicleID: "C_123"})
	require.NoError(s.T(), err)

	s.encoder.On("VerifyKnownMinorVersion", message.Type, message.DataSchemaVersion).Return(nil)
	schema := "user-created/1.0"
	s.encoder.On("EncodeMessageType", message.Type, message.DataSchemaVersion).Return(schema)

	metaAttrs := MetaAttributes{
		message.Metadata.Timestamp,
		message.Metadata.Publisher,
		message.Metadata.Headers,
		message.ID,
		schema,
		semver.MustParse("1.0"),
	}

	s.encoder.On("EncodePayload", message.Data, *s.settings.UseTransportMessageAttributes, metaAttrs).
		Return([]byte(""), map[string]string{}, errors.New("can't serialize data"))

	_, _, err = s.validator.Serialize(message)
	s.EqualError(err, "can't serialize data")

	s.encoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestSerializeValidationFailure() {
	message, err := NewMessage(s.settings, "user-created", "1.0", nil, fakeHedwigDataField{VehicleID: "C_123"})
	require.NoError(s.T(), err)

	s.encoder.On("VerifyKnownMinorVersion", message.Type, message.DataSchemaVersion).Return(nil)
	schema := "user-created/1.0"
	s.encoder.On("EncodeMessageType", message.Type, message.DataSchemaVersion).Return(schema)

	metaAttrs := MetaAttributes{
		message.Metadata.Timestamp,
		message.Metadata.Publisher,
		message.Metadata.Headers,
		message.ID,
		schema,
		semver.MustParse("1.0"),
	}

	payload := []byte("user-created/1.0 C_123")
	attributes := map[string]string{"hedwig_id": "123"}

	s.encoder.On("EncodePayload", message.Data, *s.settings.UseTransportMessageAttributes, metaAttrs).Return(payload, attributes, nil)

	s.encoder.On("ExtractData", payload, attributes, *s.settings.UseTransportMessageAttributes).
		Return(MetaAttributes{}, nil, errors.New("invalid payload"))

	_, _, err = s.validator.Serialize(message)
	s.EqualError(err, "invalid payload")

	s.encoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestDeserialize() {
	message, err := NewMessage(s.settings, "user-created", "1.0", nil, fakeHedwigDataField{VehicleID: "C_123"})
	require.NoError(s.T(), err)

	schema := "user-created/1.0"

	metaAttrs := MetaAttributes{
		message.Metadata.Timestamp,
		message.Metadata.Publisher,
		message.Metadata.Headers,
		message.ID,
		schema,
		semver.MustParse("1.0"),
	}

	payload := []byte("user-created/1.0 C_123")
	attributes := map[string]string{"hedwig_id": "123"}
	data := map[string]string{"vehicle_id": "C_123"}

	s.encoder.On("ExtractData", payload, attributes, *s.settings.UseTransportMessageAttributes).Return(metaAttrs, data, nil)
	s.encoder.On("DecodeMessageType", schema).Return(message.Type, message.DataSchemaVersion, nil)
	s.encoder.On("DecodeData", metaAttrs, message.Type, message.DataSchemaVersion, data).Return(message.Data, nil)

	returnedMessage, err := s.validator.Deserialize(payload, attributes, nil)
	assert.NoError(s.T(), err)
	assert.Equal(s.T(), returnedMessage, message)

	s.encoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestDeserializeExtractionFailure() {
	payload := []byte("user-created/1.0 C_123")
	attributes := map[string]string{"hedwig_id": "123"}

	s.encoder.On("ExtractData", payload, attributes, *s.settings.UseTransportMessageAttributes).
		Return(MetaAttributes{}, nil, errors.New("invalid payload"))

	_, err := s.validator.Deserialize(payload, attributes, nil)
	s.EqualError(err, "invalid payload")

	s.encoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestDeserializeUnknownMessageType() {
	message, err := NewMessage(s.settings, "user-created", "1.0", nil, fakeHedwigDataField{VehicleID: "C_123"})
	require.NoError(s.T(), err)

	schema := "user-created/1.0"

	metaAttrs := MetaAttributes{
		message.Metadata.Timestamp,
		message.Metadata.Publisher,
		message.Metadata.Headers,
		message.ID,
		schema,
		semver.MustParse("1.0"),
	}

	payload := []byte("user-created/1.0 C_123")
	attributes := map[string]string{"hedwig_id": "123"}
	data := map[string]string{"vehicle_id": "C_123"}

	s.encoder.On("ExtractData", payload, attributes, *s.settings.UseTransportMessageAttributes).Return(metaAttrs, data, nil)
	s.encoder.On("DecodeMessageType", schema).Return(message.Type, message.DataSchemaVersion, errors.New("invalid message type"))

	_, err = s.validator.Deserialize(payload, attributes, nil)
	s.EqualError(err, "invalid message type")

	s.encoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestDeserializeUnknownFormatVersion() {
	message, err := NewMessage(s.settings, "user-created", "1.0", nil, fakeHedwigDataField{VehicleID: "C_123"})
	require.NoError(s.T(), err)

	schema := "user-created/1.0"

	metaAttrs := MetaAttributes{
		message.Metadata.Timestamp,
		message.Metadata.Publisher,
		message.Metadata.Headers,
		message.ID,
		schema,
		semver.MustParse("1.1"),
	}

	payload := []byte("user-created/1.0 C_123")
	attributes := map[string]string{"hedwig_id": "123"}
	data := map[string]string{"vehicle_id": "C_123"}

	s.encoder.On("ExtractData", payload, attributes, *s.settings.UseTransportMessageAttributes).Return(metaAttrs, data, nil)

	_, err = s.validator.Deserialize(payload, attributes, nil)
	s.EqualError(err, "Invalid format version: 1.1")

	s.encoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestDeserializeInvalidData() {
	message, err := NewMessage(s.settings, "user-created", "1.0", nil, fakeHedwigDataField{VehicleID: "C_123"})
	require.NoError(s.T(), err)

	schema := "user-created/1.0"

	metaAttrs := MetaAttributes{
		message.Metadata.Timestamp,
		message.Metadata.Publisher,
		message.Metadata.Headers,
		message.ID,
		schema,
		semver.MustParse("1.0"),
	}

	payload := []byte("user-created/1.0 C_123")
	attributes := map[string]string{"hedwig_id": "123"}
	data := map[string]string{"vehicle_id": "C_123"}

	s.encoder.On("ExtractData", payload, attributes, *s.settings.UseTransportMessageAttributes).Return(metaAttrs, data, nil)
	s.encoder.On("DecodeMessageType", schema).Return(message.Type, message.DataSchemaVersion, nil)
	s.encoder.On("DecodeData", metaAttrs, message.Type, message.DataSchemaVersion, data).Return(message.Data, errors.New("invalid data"))

	_, err = s.validator.Deserialize(payload, attributes, nil)
	s.EqualError(err, "invalid data")

	s.encoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestDeserializeInvalidHeaders() {
	message, err := NewMessage(s.settings, "user-created", "1.0", nil, fakeHedwigDataField{VehicleID: "C_123"})
	require.NoError(s.T(), err)

	schema := "user-created/1.0"

	metaAttrs := MetaAttributes{
		message.Metadata.Timestamp,
		message.Metadata.Publisher,
		map[string]string{"hedwig_id": "123"},
		message.ID,
		schema,
		semver.MustParse("1.0"),
	}

	payload := []byte("user-created/1.0 C_123")
	attributes := map[string]string{"hedwig_id": "123"}
	data := map[string]string{"vehicle_id": "C_123"}

	s.encoder.On("ExtractData", payload, attributes, *s.settings.UseTransportMessageAttributes).Return(metaAttrs, data, nil)

	_, err = s.validator.Deserialize(payload, attributes, nil)
	s.EqualError(err, "invalid header key: 'hedwig_id' - can't begin with reserved namespace 'hedwig_'")

	s.encoder.AssertExpectations(s.T())
}

func (s *ValidatorTestSuite) TestNew() {
	assert.NotNil(s.T(), s.validator)
}

type ValidatorTestSuite struct {
	suite.Suite
	validator IMessageValidator
	backend   *fakeBackend
	callback  *fakeCallback
	settings  *Settings
	encoder   *fakeEncoder
}

func (s *ValidatorTestSuite) SetupTest() {
	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		QueueName:    "dev-myapp",
		MessageRouting: map[MessageRouteKey]string{
			{
				MessageType:         "user-created",
				MessageMajorVersion: 1,
			}: "dev-user-created-v1",
		},
	}
	backend := &fakeBackend{}
	encoder := &fakeEncoder{}

	s.validator = NewMessageValidator(settings, encoder)
	s.encoder = encoder
	s.backend = backend
	s.settings = settings
}

func TestValidatorTestSuite(t *testing.T) {
	suite.Run(t, &ValidatorTestSuite{})
}
