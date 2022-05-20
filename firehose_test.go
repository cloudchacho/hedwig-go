package hedwig

import (
	"bytes"
	"testing"
	"time"

	"github.com/Masterminds/semver"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

func (s *FirehoseTestSuite) TestSerializeFirehose() {
	s.encoder.On("IsBinary").Return(true)
	s.encoder.On("VerifyKnownMinorVersion", s.message.Type, s.message.DataSchemaVersion).Return(nil)
	schema := "user-created/1.0"
	s.encoder.On("EncodeMessageType", s.message.Type, s.message.DataSchemaVersion).Return(schema)

	payload := []byte("user-created/1.0 C_123")

	s.encoder.On("EncodeData", s.message.Data, false, s.metaAttrs).
		Return(payload, nil)

	s.decoder.On("DecodeMessageType", schema).Return(s.message.Type, s.message.DataSchemaVersion, nil)
	s.decoder.On("DecodeData", s.message.Type, s.message.DataSchemaVersion, payload).
		Return(s.message.Data, nil)
	s.decoder.On("ExtractData", payload, map[string]string(nil)).Return(s.metaAttrs, payload, nil)
	s.decoder.On("ExtractData", payload, map[string]string{"foo": "bar"}).Return(s.metaAttrs, payload, nil)
	res, err := s.firehose.Serialize(s.message)
	s.Nil(err)
	expMsgLength := []byte{22, 0, 0, 0, 0, 0, 0, 0}
	expectedMsg := []byte("user-created/1.0 C_123")
	expected := append(expMsgLength, expectedMsg...)
	s.Equal(res, expected)
}

func (s *FirehoseTestSuite) TestSerializeFirehoseNonBinary() {
	s.encoder.On("IsBinary").Return(false)
	s.encoder.On("VerifyKnownMinorVersion", s.message.Type, s.message.DataSchemaVersion).Return(nil)
	schema := "user-created/1.0"
	s.encoder.On("EncodeMessageType", s.message.Type, s.message.DataSchemaVersion).Return(schema)

	payload := []byte("user-created/1.0 C_123")

	s.encoder.On("EncodeData", s.message.Data, false, s.metaAttrs).
		Return(payload, nil)

	s.decoder.On("DecodeMessageType", schema).Return(s.message.Type, s.message.DataSchemaVersion, nil)
	s.decoder.On("DecodeData", s.message.Type, s.message.DataSchemaVersion, payload).
		Return(s.message.Data, nil)
	s.decoder.On("ExtractData", payload, map[string]string(nil)).Return(s.metaAttrs, payload, nil)
	s.decoder.On("ExtractData", payload, map[string]string{"foo": "bar"}).Return(s.metaAttrs, payload, nil)
	res, err := s.firehose.Serialize(s.message)
	s.Nil(err)
	expected := []byte("user-created/1.0 C_123\n")
	s.Equal(res, expected)
}

func (s *FirehoseTestSuite) TestSerializeFirehoseError() {
	s.encoder.On("IsBinary").Return(true)
	s.encoder.On("VerifyKnownMinorVersion", s.message.Type, s.message.DataSchemaVersion).Return(errors.New("bad version"))
	schema := "user-created/1.0"
	s.encoder.On("EncodeMessageType", s.message.Type, s.message.DataSchemaVersion).Return(schema)

	payload := []byte("user-created/1.0 C_123")

	s.encoder.On("EncodeData", s.message.Data, false, s.metaAttrs).
		Return(payload, nil)

	s.decoder.On("DecodeMessageType", schema).Return(s.message.Type, s.message.DataSchemaVersion, nil)
	s.decoder.On("DecodeData", s.message.Type, s.message.DataSchemaVersion, payload).
		Return(s.message.Data, nil)
	s.decoder.On("ExtractData", payload, nil).Return(s.metaAttrs, payload, nil)
	s.decoder.On("ExtractData", payload, map[string]string{}).Return(s.metaAttrs, payload, nil)
	_, err := s.firehose.Serialize(s.message)
	s.EqualError(err, "bad version")
}

func (s *FirehoseTestSuite) TestDeSerializeFirehose() {
	s.encoder.On("IsBinary").Return(true)
	// first 8 bytes is length of message (22) in this case
	payload := []byte("user-created/1.0 C_123")
	schema := "user-created/1.0"

	s.encoder.On("VerifyKnownMinorVersion", s.message.Type, s.message.DataSchemaVersion).Return(nil)
	s.encoder.On("EncodeMessageType", s.message.Type, s.message.DataSchemaVersion).Return(schema)
	s.encoder.On("EncodeData", s.message.Data, false, s.metaAttrs).
		Return(payload, nil)

	s.decoder.On("ExtractData", payload, map[string]string(nil)).Return(s.metaAttrs, payload, nil)
	s.decoder.On("ExtractData", payload, map[string]string{"foo": "bar"}).Return(s.metaAttrs, payload, nil)
	s.decoder.On("DecodeMessageType", schema).Return(s.message.Type, s.message.DataSchemaVersion, nil)
	s.decoder.On("DecodeData", s.message.Type, s.message.DataSchemaVersion, payload).
		Return(s.message.Data, nil)
	line, err := s.firehose.Serialize(s.message)
	multiple := append(line, line...)
	r := bytes.NewReader(multiple)
	s.Nil(err)
	res, err := s.firehose.Deserialize(r)
	s.Nil(err)
	s.Equal(len(res), 2)
	s.Equal(res[0], *s.message)
	s.Equal(res[1], *s.message)

}

func (s *FirehoseTestSuite) TestDeSerializeFirehoseNonBinary() {
	s.encoder.On("IsBinary").Return(false)
	// first 8 bytes is length of message (22) in this case
	payload := []byte("user-created/1.0 C_123")
	schema := "user-created/1.0"

	s.encoder.On("VerifyKnownMinorVersion", s.message.Type, s.message.DataSchemaVersion).Return(nil)
	s.encoder.On("EncodeMessageType", s.message.Type, s.message.DataSchemaVersion).Return(schema)
	s.encoder.On("EncodeData", s.message.Data, false, s.metaAttrs).
		Return(payload, nil)

	s.decoder.On("ExtractData", payload, map[string]string(nil)).Return(s.metaAttrs, payload, nil)
	s.decoder.On("ExtractData", payload, map[string]string{"foo": "bar"}).Return(s.metaAttrs, payload, nil)
	s.decoder.On("DecodeMessageType", schema).Return(s.message.Type, s.message.DataSchemaVersion, nil)
	s.decoder.On("DecodeData", s.message.Type, s.message.DataSchemaVersion, payload).
		Return(s.message.Data, nil)
	line, err := s.firehose.Serialize(s.message)
	multiple := append(line, line...)
	r := bytes.NewReader(multiple)
	s.Nil(err)
	res, err := s.firehose.Deserialize(r)
	s.Nil(err)
	s.Equal(len(res), 2)
	s.Equal(res[0], *s.message)
	s.Equal(res[1], *s.message)

}

func (s *FirehoseTestSuite) TestNew() {
	assert.NotNil(s.T(), s.firehose)
}

type FirehoseTestSuite struct {
	suite.Suite
	firehose   *Firehose
	encoder    *fakeEncoder
	decoder    *fakeDecoder
	attributes map[string]string
	message    *Message
	metaAttrs  MetaAttributes
}

func (s *FirehoseTestSuite) SetupTest() {
	encoder := &fakeEncoder{}
	decoder := &fakeDecoder{}
	message, err := NewMessage(
		"user-created", "1.0", map[string]string{"foo": "bar"}, fakeHedwigDataField{VehicleID: "C_123"}, "myapp")
	require.NoError(s.T(), err)
	// fixed timestamp
	message.Metadata.Timestamp = time.Unix(1621550514, 123000000)
	schema := "user-created/1.0"

	s.firehose = NewFirehose(encoder, decoder)
	s.encoder = encoder
	s.decoder = decoder
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

func TestFirehoseTestSuite(t *testing.T) {
	suite.Run(t, &FirehoseTestSuite{})
}
