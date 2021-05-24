package jsonschema

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/Masterminds/semver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudchacho/hedwig-go"
)

// FakeHedwigDataField is a fake data field for testing
type FakeHedwigDataField struct {
	VehicleID string `json:"vehicle_id"`
}

func newFakeHedwigDataField() interface{} {
	return new(FakeHedwigDataField)
}

func (s *EncoderTestSuite) TestFormatHumanUUID() {
	testSchema := []byte(`{
    "id": "https://hedwig.automatic.com/schema",
    "$schema": "http://json-schema.org/draft-04/schema#",
    "description": "Test Schema for Hedwig messages",
    "schemas": {
        "vehicle_created": {
            "1.*": {
				"type": "string",
				"format": "human-uuid",
				"x-version": "1.0"
			}
		}
	}
}`)
	newString := func() interface{} { return new(string) }
	encoder, err := NewEncoderFromBytes(testSchema, hedwig.DataFactoryRegistry{hedwig.DataRegistryKey{"vehicle_created", 1}: newString})
	s.NoError(err)

	data := json.RawMessage(`"6cac5588-24cc-4b4f-bbf9-7dc0ce93f96e"`)

	decoded, err := encoder.DecodeData(
		hedwig.MetaAttributes{Schema: "https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0"},
		"vehicle_created",
		semver.MustParse("1.0"),
		data,
	)
	s.NoError(err)
	s.Equal("6cac5588-24cc-4b4f-bbf9-7dc0ce93f96e", *decoded.(*string))

	data = json.RawMessage(`"abcd"`)
	_, err = encoder.DecodeData(
		hedwig.MetaAttributes{Schema: "https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0"},
		"vehicle_created",
		semver.MustParse("1.0"),
		data,
	)
	s.Error(err)

	data = json.RawMessage(`"yyyyyyyy-tttt-416a-92ed-420e62b33eb5"`)
	_, err = encoder.DecodeData(
		hedwig.MetaAttributes{Schema: "https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0"},
		"vehicle_created",
		semver.MustParse("1.0"),
		data,
	)
	s.Error(err)
}

func (s *EncoderTestSuite) TestInvalidXVersion() {
	testSchema := []byte(`{
    "id": "https://hedwig.automatic.com/schema",
    "$schema": "http://json-schema.org/draft-04/schema#",
    "description": "Test Schema for Hedwig messages",
    "schemas": {
        "vehicle_created": {
            "1.*": {
				"type": "string"
			}
		}
	}
}`)
	newString := func() interface{} { return new(string) }
	_, err := NewEncoderFromBytes(testSchema, hedwig.DataFactoryRegistry{hedwig.DataRegistryKey{"vehicle_created", 1}: newString})
	s.EqualError(err, "Missing x-version from schema definition")

	testSchema = []byte(`{
    "id": "https://hedwig.automatic.com/schema",
    "$schema": "http://json-schema.org/draft-04/schema#",
    "description": "Test Schema for Hedwig messages",
    "schemas": {
        "vehicle_created": {
            "1.*": {
				"type": "string",
				"x-version": "foobar"
			}
		}
	}
}`)
	_, err = NewEncoderFromBytes(testSchema, hedwig.DataFactoryRegistry{hedwig.DataRegistryKey{"vehicle_created", 1}: newString})
	s.EqualError(err, "invalid value for x-version: foobar, must be semver")

	testSchema = []byte(`{
    "id": "https://hedwig.automatic.com/schema",
    "$schema": "http://json-schema.org/draft-04/schema#",
    "description": "Test Schema for Hedwig messages",
    "schemas": {
        "vehicle_created": {
            "1.*": {
				"type": "string",
				"x-version": 1
			}
		}
	}
}`)
	_, err = NewEncoderFromBytes(testSchema, hedwig.DataFactoryRegistry{hedwig.DataRegistryKey{"vehicle_created", 1}: newString})
	s.Error(err)
}

func (s *EncoderTestSuite) TestVerifyKnownMinorVersion() {
	err := s.encoder.VerifyKnownMinorVersion("vehicle_created", semver.MustParse("1.0"))
	s.NoError(err)

	err = s.encoder.VerifyKnownMinorVersion("vehicle_created", semver.MustParse("1.1"))
	s.Error(err)

	err = s.encoder.VerifyKnownMinorVersion("foobar", semver.MustParse("1.0"))
	s.Error(err)
}

func (s *EncoderTestSuite) TestEncodeMessageType() {
	messageType, err := s.encoder.EncodeMessageType("vehicle_created", semver.MustParse("1.0"))
	s.NoError(err)
	s.Equal(messageType, "https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0")
}

func (s *EncoderTestSuite) TestEncodePayload() {
	data := FakeHedwigDataField{VehicleID: "C_123"}
	metaAttrs := hedwig.MetaAttributes{
		Timestamp:     time.Unix(1621550514, 0),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "123",
		Schema:        "https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	}
	payload, attributes, err := s.encoder.EncodePayload(data, true, metaAttrs)
	s.NoError(err)
	s.Equal(string(payload), "{\"vehicle_id\":\"C_123\"}")
	s.Equal(attributes, map[string]string{
		"hedwig_message_timestamp": "1621550514",
		"hedwig_publisher":         "myapp",
		"foo":                      "bar",
		"hedwig_id":                "123",
		"hedwig_schema":            "https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0",
		"hedwig_format_version":    "1.0",
	})
}

func (s *EncoderTestSuite) TestEncodePayloadContainerized() {
	data := FakeHedwigDataField{VehicleID: "C_123"}
	metaAttrs := hedwig.MetaAttributes{
		Timestamp:     time.Unix(1621550514, 0),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "123",
		Schema:        "https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	}
	payload, attributes, err := s.encoder.EncodePayload(data, false, metaAttrs)
	s.NoError(err)
	s.Equal(string(payload), "{\"format_version\":\"1.0\","+
		"\"schema\":\"https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0\","+
		"\"id\":\"123\","+
		"\"metadata\":{\"Timestamp\":1621550514000,\"Publisher\":\"myapp\",\"Headers\":{\"foo\":\"bar\"}},"+
		"\"data\":{\"vehicle_id\":\"C_123\"}}")
	s.Equal(attributes, map[string]string{
		"foo": "bar",
	})
}

func (s *EncoderTestSuite) TestEncodePayloadFailInvalidData() {
	data := make(chan bool)
	metaAttrs := hedwig.MetaAttributes{
		Timestamp:     time.Unix(1621550514, 0),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "123",
		Schema:        "https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	}
	_, _, err := s.encoder.EncodePayload(data, true, metaAttrs)
	s.Error(err)
}

func (s *EncoderTestSuite) TestExtractData() {
	payload := []byte(`{"vehicle_id":"C_123"}`)
	attributes := map[string]string{
		"hedwig_message_timestamp": "1621550514123",
		"hedwig_publisher":         "myapp",
		"foo":                      "bar",
		"hedwig_id":                "123",
		"hedwig_schema":            "https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0",
		"hedwig_format_version":    "1.0",
	}
	metaAttrs, extractedData, err := s.encoder.ExtractData(payload, attributes, true)
	s.NoError(err)
	s.Equal(metaAttrs, hedwig.MetaAttributes{
		Timestamp:     time.Unix(1621550514, 0),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "123",
		Schema:        "https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	})
	s.Equal(string(extractedData.(json.RawMessage)), "{\"vehicle_id\":\"C_123\"}")
}

func (s *EncoderTestSuite) TestExtractDataValidation() {
	payload := []byte(`{"vehicle_id":"C_123"}`)
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
			_, _, err := s.encoder.ExtractData(payload, badAttributes, true)
			s.Error(err)
		})
	}
}

func (s *EncoderTestSuite) TestExtractDataContainerized() {
	payload := []byte(`{"format_version":"1.0",
		"schema":"https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0",
		"id":"d70a641e-14ab-32e4-a790-459bd36de532",
		"metadata":{"Timestamp":1621550514000,"Publisher":"myapp","Headers":{"foo":"bar"}},
		"data":{"vehicle_id":"C_123"}}`)
	attributes := map[string]string{
		"foo": "bar",
	}
	metaAttrs, extractedData, err := s.encoder.ExtractData(payload, attributes, false)
	s.NoError(err)
	s.Equal(metaAttrs, hedwig.MetaAttributes{
		Timestamp:     time.Unix(1621550514, 0),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "d70a641e-14ab-32e4-a790-459bd36de532",
		Schema:        "https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	})
	s.Equal(string(extractedData.(json.RawMessage)), "{\"vehicle_id\":\"C_123\"}")
}

func (s *EncoderTestSuite) TestExtractDataContainerizedInvalid() {
	payload := []byte(`{"schema":"https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0",
		"id":"d70a641e-14ab-32e4-a790-459bd36de532",
		"metadata":{"Timestamp":1621550514000,"Publisher":"myapp","Headers":{"foo":"bar"}},
		"data":{"vehicle_id":"C_123"}}`)
	attributes := map[string]string{
		"foo": "bar",
	}
	_, _, err := s.encoder.ExtractData(payload, attributes, false)
	s.Error(err)
}

func (s *EncoderTestSuite) TestDecodeMessageType() {
	schema := "https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0"
	messageType, version, err := s.encoder.DecodeMessageType(schema)
	s.NoError(err)
	s.Equal(messageType, "vehicle_created")
	s.Equal(version, semver.MustParse("1.0"))

	schema = "vehicle_created 1.0"
	_, _, err = s.encoder.DecodeMessageType(schema)
	s.Error(err)
}

func (s *EncoderTestSuite) TestDecodeData() {
	data := json.RawMessage([]byte(`{"vehicle_id":"C_1234567890123456"}`))
	messageType := "vehicle_created"
	version := semver.MustParse("1.0")
	metaAttrs := hedwig.MetaAttributes{
		Timestamp:     time.Unix(1621550514, 0),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "d70a641e-14ab-32e4-a790-459bd36de532",
		Schema:        "https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	}
	decodedData, err := s.encoder.DecodeData(metaAttrs, messageType, version, data)
	s.NoError(err)
	s.Equal(decodedData, &FakeHedwigDataField{VehicleID: "C_1234567890123456"})
}

func (s *EncoderTestSuite) TestDecodeDataUnknownType() {
	data := json.RawMessage([]byte(`{"vehicle_id":"C_1234567890123456"}`))
	messageType := "unknown"
	version := semver.MustParse("1.0")
	metaAttrs := hedwig.MetaAttributes{
		Timestamp:     time.Unix(1621550514, 0),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "d70a641e-14ab-32e4-a790-459bd36de532",
		Schema:        "https://hedwig.automatic.com/schema#/schemas/unknown/1.0",
		FormatVersion: semver.MustParse("1.0"),
	}
	_, err := s.encoder.DecodeData(metaAttrs, messageType, version, data)
	s.Error(err)
}

func (s *EncoderTestSuite) TestDecodeDataUnknownVersion() {
	data := json.RawMessage([]byte(`{"vehicle_id":"C_1234567890123456"}`))
	messageType := "vehicle_created"
	version := semver.MustParse("2.0")
	metaAttrs := hedwig.MetaAttributes{
		Timestamp:     time.Unix(1621550514, 0),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "d70a641e-14ab-32e4-a790-459bd36de532",
		Schema:        "https://hedwig.automatic.com/schema#/schemas/vehicle_created/2.0",
		FormatVersion: semver.MustParse("1.0"),
	}
	_, err := s.encoder.DecodeData(metaAttrs, messageType, version, data)
	s.Error(err)
}

func (s *EncoderTestSuite) TestDecodeDataInvalidSchema() {
	data := json.RawMessage([]byte(`{"vehicle_id":"C_1234567890123456"}`))
	messageType := "vehicle_created"
	version := semver.MustParse("2.0")
	metaAttrs := hedwig.MetaAttributes{
		Timestamp:     time.Unix(1621550514, 0),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "d70a641e-14ab-32e4-a790-459bd36de532",
		Schema:        "vehicle_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	}
	_, err := s.encoder.DecodeData(metaAttrs, messageType, version, data)
	s.Error(err)
}

func (s *EncoderTestSuite) TestDecodeDataInvalidDataType() {
	data := []byte(`{"vehicle_id":"C_1234567890123456"}`)
	messageType := "vehicle_created"
	version := semver.MustParse("2.0")
	metaAttrs := hedwig.MetaAttributes{
		Timestamp:     time.Unix(1621550514, 0),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "d70a641e-14ab-32e4-a790-459bd36de532",
		Schema:        "https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	}
	_, err := s.encoder.DecodeData(metaAttrs, messageType, version, data)
	s.Error(err)
}

func (s *EncoderTestSuite) TestDecodeDataInvalidData() {
	data := []byte(`{}`)
	messageType := "vehicle_created"
	version := semver.MustParse("2.0")
	metaAttrs := hedwig.MetaAttributes{
		Timestamp:     time.Unix(1621550514, 0),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "d70a641e-14ab-32e4-a790-459bd36de532",
		Schema:        "https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	}
	_, err := s.encoder.DecodeData(metaAttrs, messageType, version, data)
	s.Error(err)
}

func (s *EncoderTestSuite) TestDecodeDataInvalidDataJSON() {
	data := []byte(`{`)
	messageType := "vehicle_created"
	version := semver.MustParse("2.0")
	metaAttrs := hedwig.MetaAttributes{
		Timestamp:     time.Unix(1621550514, 0),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "d70a641e-14ab-32e4-a790-459bd36de532",
		Schema:        "https://hedwig.automatic.com/schema#/schemas/vehicle_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	}
	_, err := s.encoder.DecodeData(metaAttrs, messageType, version, data)
	s.Error(err)
}

func (s *EncoderTestSuite) TestDecodeDataInvalidDataFactory() {
	data := []byte(`{`)
	messageType := "trip_created"
	version := semver.MustParse("2.0")
	metaAttrs := hedwig.MetaAttributes{
		Timestamp:     time.Unix(1621550514, 0),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "d70a641e-14ab-32e4-a790-459bd36de532",
		Schema:        "https://hedwig.automatic.com/schema#/schemas/trip_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	}
	_, err := s.encoder.DecodeData(metaAttrs, messageType, version, data)
	s.Error(err)
}

func TestInvalidSchemaNoXVersion(t *testing.T) {
	assertions := assert.New(t)
	schemaMissingXversions := `
	{
		"id": "https://hedwig.automatic.com/schema",
		"$schema": "http://json-schema.org/draft-04/schema#",
		"description": "Test Schema for Hedwig messages",
		"schemas": {
			"trip_created": {
				"1": {
					"description": "This is a message type",
					"type": "object",
					"required": [
						"vehicle_id",
						"user_id"
					],
					"properties": {
						"vehicle_id": {
							"type": "string"
						},
						"user_id": {
							"type": "string"
						},
						"vin": {
							"type": "string"
						}
					}
				}
			}
		}
	}
	`
	v, err := NewEncoderFromBytes([]byte(schemaMissingXversions), hedwig.DataFactoryRegistry{})
	assertions.Nil(v)
	assertions.Error(err, "x-versions not defined for message for schemaURL: https://hedwig.automatic.com/schema/schemas/trip_created/1")
}

func (s *EncoderTestSuite) TestNew() {
	assert.NotNil(s.T(), s.encoder)
}

type EncoderTestSuite struct {
	suite.Suite
	encoder *messageEncoder
}

func (s *EncoderTestSuite) SetupTest() {
	registry := hedwig.DataFactoryRegistry{hedwig.DataRegistryKey{"vehicle_created", 1}: newFakeHedwigDataField}
	encoder, err := NewMessageEncoder("schema.json", registry)
	require.NoError(s.T(), err)

	s.encoder = encoder.(*messageEncoder)
}

func TestEncoderTestSuite(t *testing.T) {
	suite.Run(t, &EncoderTestSuite{})
}
