package protobuf_test

import (
	"testing"
	"time"

	"github.com/Masterminds/semver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/cloudchacho/hedwig-go"
	"github.com/cloudchacho/hedwig-go/protobuf"
	"github.com/cloudchacho/hedwig-go/protobuf/internal"
)

func (s *EncoderTestSuite) TestVerifyKnownMinorVersion() {
	err := s.encoder.VerifyKnownMinorVersion("vehicle_created", semver.MustParse("1.0"))
	s.NoError(err)

	err = s.encoder.VerifyKnownMinorVersion("vehicle_created", semver.MustParse("1.1"))
	s.Error(err)

	err = s.encoder.VerifyKnownMinorVersion("foobar", semver.MustParse("1.0"))
	s.Error(err)
}

func (s *EncoderTestSuite) TestEncodeMessageType() {
	messageType := s.encoder.EncodeMessageType("vehicle_created", semver.MustParse("1.0"))
	s.Equal(messageType, "vehicle_created/1.0")
}

func (s *EncoderTestSuite) TestEncodeData() {
	metaAttrs := hedwig.MetaAttributes{
		Timestamp:     time.Unix(1621550514, 0),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "123",
		Schema:        "vehicle_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	}
	vehicleID := "C_123"
	data := &internal.VehicleCreatedV1{VehicleId: &vehicleID}
	payload, err := s.encoder.EncodeData(data, true, metaAttrs)
	s.NoError(err)
	serializedData := &internal.VehicleCreatedV1{}
	err = proto.Unmarshal(payload, serializedData)
	s.NoError(err)
	s.Equal(serializedData.String(), data.String())
}

func (s *EncoderTestSuite) TestEncodeDataContainerized() {
	metaAttrs := hedwig.MetaAttributes{
		Timestamp:     time.Unix(1621550514, 123000000),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "123",
		Schema:        "vehicle_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	}
	vehicleID := "C_123"
	data := &internal.VehicleCreatedV1{VehicleId: &vehicleID}
	dataTyped, err := anypb.New(data)
	s.Require().NoError(err)
	payloadMsg := &protobuf.PayloadV1{
		FormatVersion: "1.0",
		Id:            "123",
		Metadata: &protobuf.MetadataV1{
			Publisher: "myapp",
			Timestamp: timestamppb.New(metaAttrs.Timestamp),
			Headers:   metaAttrs.Headers,
		},
		Schema: metaAttrs.Schema,
		Data:   dataTyped,
	}
	payload, err := s.encoder.EncodeData(data, false, metaAttrs)
	s.NoError(err)
	serializedPayload := &protobuf.PayloadV1{}
	err = proto.Unmarshal(payload, serializedPayload)
	s.Require().NoError(err)
	s.Equal(payloadMsg.String(), serializedPayload.String())
}

func (s *EncoderTestSuite) TestEncodeDataFailInvalidData() {
	data := 1
	metaAttrs := hedwig.MetaAttributes{
		Timestamp:     time.Unix(1621550514, 123000000),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "123",
		Schema:        "vehicle_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	}
	_, err := s.encoder.EncodeData(data, true, metaAttrs)
	s.Error(err)
}

func (s *EncoderTestSuite) TestExtractData() {
	vehicleID := "C_123"
	data := &internal.VehicleCreatedV1{VehicleId: &vehicleID}
	dataTyped, err := anypb.New(data)
	s.Require().NoError(err)
	payloadMsg := &protobuf.PayloadV1{
		FormatVersion: "1.0",
		Id:            "d70a641e-14ab-32e4-a790-459bd36de532",
		Metadata: &protobuf.MetadataV1{
			Publisher: "myapp",
			Timestamp: timestamppb.New(time.Unix(1621550514, 123000000)),
			Headers:   map[string]string{"foo": "bar"},
		},
		Schema: "vehicle_created/1.0",
		Data:   dataTyped,
	}
	payload, err := proto.Marshal(payloadMsg)
	s.Require().NoError(err)
	attributes := map[string]string{
		"foo": "bar",
	}
	metaAttrs, extractedData, err := s.encoder.ExtractData(payload, attributes)
	s.NoError(err)
	s.Equal(hedwig.MetaAttributes{
		Timestamp:     time.Unix(1621550514, 123000000).UTC(),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "d70a641e-14ab-32e4-a790-459bd36de532",
		Schema:        "vehicle_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	}, metaAttrs)
	s.Equal(dataTyped.String(), extractedData.(*anypb.Any).String())
}

func (s *EncoderTestSuite) TestExtractDataInvalid() {
	payload := []byte(`foobar`)
	attributes := map[string]string{
		"foo": "bar",
	}
	_, _, err := s.encoder.ExtractData(payload, attributes)
	s.Error(err)
}

func (s *EncoderTestSuite) TestExtractDataInvalidFormatVersion() {
	vehicleID := "C_123"
	data := &internal.VehicleCreatedV1{VehicleId: &vehicleID}
	dataTyped, err := anypb.New(data)
	s.Require().NoError(err)
	payloadMsg := &protobuf.PayloadV1{
		FormatVersion: "foobar",
		Id:            "d70a641e-14ab-32e4-a790-459bd36de532",
		Metadata: &protobuf.MetadataV1{
			Publisher: "myapp",
			Timestamp: timestamppb.New(time.Unix(1621550514, 123000000)),
			Headers:   map[string]string{"foo": "bar"},
		},
		Schema: "vehicle_created/1.0",
		Data:   dataTyped,
	}
	payload, err := proto.Marshal(payloadMsg)
	s.Require().NoError(err)
	attributes := map[string]string{
		"foo": "bar",
	}
	_, _, err = s.encoder.ExtractData(payload, attributes)
	s.Error(err)
}

func (s *EncoderTestSuite) TestDecodeMessageType() {
	schema := "vehicle_created/1.0"
	messageType, version, err := s.encoder.DecodeMessageType(schema)
	s.NoError(err)
	s.Equal(messageType, "vehicle_created")
	s.Equal(version, semver.MustParse("1.0"))

	schema = "https://github.com/cloudchacho/hedwig-go/schema#/schemas/vehicle_created 1.0"
	_, _, err = s.encoder.DecodeMessageType(schema)
	s.Error(err)
}

func (s *EncoderTestSuite) TestDecodeData() {
	vehicleID := "C_123"
	dataMsg := &internal.VehicleCreatedV1{VehicleId: &vehicleID}
	data, err := proto.Marshal(dataMsg)
	s.Require().NoError(err)
	messageType := "vehicle_created"
	version := semver.MustParse("1.0")
	decodedDataMsg, err := s.encoder.DecodeData(messageType, version, data)
	s.NoError(err)
	s.Equal(dataMsg.String(), decodedDataMsg.(*internal.VehicleCreatedV1).String())
}

func (s *EncoderTestSuite) TestDecodeDataContainerized() {
	vehicleID := "C_123"
	data := &internal.VehicleCreatedV1{VehicleId: &vehicleID}
	dataTyped, err := anypb.New(data)
	s.Require().NoError(err)
	messageType := "vehicle_created"
	version := semver.MustParse("1.0")
	decodedDataMsg, err := s.encoder.DecodeData(messageType, version, dataTyped)
	s.NoError(err)
	s.Equal(data.String(), decodedDataMsg.(*internal.VehicleCreatedV1).String())
}

func (s *EncoderTestSuite) TestDecodeDataUnknownType() {
	vehicleID := "C_123"
	dataMsg := &internal.VehicleCreatedV1{VehicleId: &vehicleID}
	data, err := proto.Marshal(dataMsg)
	s.Require().NoError(err)
	messageType := "unknown"
	version := semver.MustParse("1.0")
	_, err = s.encoder.DecodeData(messageType, version, data)
	s.Error(err)
}

func (s *EncoderTestSuite) TestDecodeDataUnknownVersion() {
	vehicleID := "C_123"
	dataMsg := &internal.VehicleCreatedV1{VehicleId: &vehicleID}
	data, err := proto.Marshal(dataMsg)
	s.Require().NoError(err)
	messageType := "vehicle_created"
	version := semver.MustParse("2.0")
	_, err = s.encoder.DecodeData(messageType, version, data)
	s.Error(err)
}

func (s *EncoderTestSuite) TestDecodeDataInvalidSchema() {
	vehicleID := "C_123"
	dataMsg := &internal.VehicleCreatedV1{VehicleId: &vehicleID}
	data, err := proto.Marshal(dataMsg)
	s.Require().NoError(err)
	messageType := "vehicle_created"
	version := semver.MustParse("2.0")
	_, err = s.encoder.DecodeData(messageType, version, data)
	s.Error(err)
}

func (s *EncoderTestSuite) TestDecodeDataInvalidDataType() {
	data := `{"vehicle_id":"C_1234567890123456"}`
	messageType := "vehicle_created"
	version := semver.MustParse("1.0")
	_, err := s.encoder.DecodeData(messageType, version, data)
	s.Error(err)
}

func (s *EncoderTestSuite) TestDecodeDataInvalidData() {
	data := []byte(`{}`)
	messageType := "vehicle_created"
	version := semver.MustParse("1.0")
	_, err := s.encoder.DecodeData(messageType, version, data)
	s.Error(err)
}

func (s *EncoderTestSuite) TestNew() {
	assert.NotNil(s.T(), s.encoder)
}

func (s *EncoderTestSuite) TestBinary() {
	s.Equal(s.encoder.IsBinary(), true)
	assert.NotNil(s.T(), s.encoder)
}

type EncoderTestSuite struct {
	suite.Suite
	encoder *protobuf.EncoderDecoder
}

func (s *EncoderTestSuite) SetupTest() {
	protoMsgs := []proto.Message{
		&internal.TripCreatedV1{},
		&internal.TripCreatedV2{},
		&internal.DeviceCreatedV1{},
		&internal.VehicleCreatedV1{},
	}
	encoder, err := protobuf.NewMessageEncoderDecoder(protoMsgs)
	require.NoError(s.T(), err)

	s.encoder = encoder
}

func TestEncoderTestSuite(t *testing.T) {
	suite.Run(t, &EncoderTestSuite{})
}

func TestNewMessageEncoderFromMessageTypes(t *testing.T) {
	assertions := assert.New(t)
	protoMsgs := map[hedwig.MessageTypeMajorVersion]protoreflect.Message{
		{MessageType: "device_created", MajorVersion: 1}: (&internal.DeviceCreated{}).ProtoReflect(),
	}
	v, err := protobuf.NewMessageEncoderDecoderFromMessageTypes(protoMsgs)
	assertions.NoError(err)
	assertions.NotNil(v)
}

func TestNewMessageEncoderFromMessageTypesInvalidMessageType(t *testing.T) {
	assertions := assert.New(t)
	invalidProtoMsgs := map[hedwig.MessageTypeMajorVersion]protoreflect.Message{
		{"", 1}: (&internal.DeviceCreated{}).ProtoReflect(),
	}
	v, err := protobuf.NewMessageEncoderDecoderFromMessageTypes(invalidProtoMsgs)
	assertions.Nil(v)
	assertions.Error(err)
}

func TestNewMessageEncoderFromMessageTypesInvalidMajorVersion(t *testing.T) {
	assertions := assert.New(t)
	invalidProtoMsgs := map[hedwig.MessageTypeMajorVersion]protoreflect.Message{
		{"device_created", 0}: (&internal.DeviceCreated{}).ProtoReflect(),
	}
	v, err := protobuf.NewMessageEncoderDecoderFromMessageTypes(invalidProtoMsgs)
	assertions.Nil(v)
	assertions.Error(err)
}

func TestNewMessageEncoderFromMessageTypesMessageTypeMismatch(t *testing.T) {
	assertions := assert.New(t)
	invalidProtoMsgs := map[hedwig.MessageTypeMajorVersion]protoreflect.Message{
		{"device_created", 1}: (&internal.VehicleCreatedV1{}).ProtoReflect(),
	}
	v, err := protobuf.NewMessageEncoderDecoderFromMessageTypes(invalidProtoMsgs)
	assertions.Nil(v)
	assertions.Error(err)
}

func TestNewMessageEncoderFromMessageTypesMajorVersionMismatch(t *testing.T) {
	assertions := assert.New(t)
	invalidProtoMsgs := map[hedwig.MessageTypeMajorVersion]protoreflect.Message{
		{"vehicle_created", 2}: (&internal.VehicleCreatedV1{}).ProtoReflect(),
	}
	v, err := protobuf.NewMessageEncoderDecoderFromMessageTypes(invalidProtoMsgs)
	assertions.Nil(v)
	assertions.Error(err)
}

func TestInvalidSchemaBadNameNoMessageType(t *testing.T) {
	assertions := assert.New(t)
	invalidProtoMsgs := []proto.Message{
		&internal.DeviceCreated{},
	}
	v, err := protobuf.NewMessageEncoderDecoder(invalidProtoMsgs)
	assertions.Nil(v)
	assertions.Error(err)
}

func TestInvalidSchemaBadNameNoMajorVersion(t *testing.T) {
	assertions := assert.New(t)
	invalidProtoMsgs := []proto.Message{
		&internal.DeviceCreatedNew{},
	}
	v, err := protobuf.NewMessageEncoderDecoder(invalidProtoMsgs)
	assertions.Nil(v)
	assertions.Error(err)
}

func TestInvalidSchemaMajorVersionMismatch(t *testing.T) {
	assertions := assert.New(t)
	invalidProtoMsgs := []proto.Message{
		&internal.TripCreatedV4{},
	}
	v, err := protobuf.NewMessageEncoderDecoder(invalidProtoMsgs)
	assertions.Nil(v)
	assertions.Error(err)
}

func TestInvalidSchemaDuplicate(t *testing.T) {
	assertions := assert.New(t)
	invalidProtoMsgs := []proto.Message{
		&internal.TripCreatedV2{},
		&internal.TripCreatedV2New{},
	}
	v, err := protobuf.NewMessageEncoderDecoder(invalidProtoMsgs)
	assertions.Nil(v)
	assertions.Error(err)
}
