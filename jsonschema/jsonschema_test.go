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

// fakeHedwigDataField is a fake data field for testing
type fakeHedwigDataField struct {
	VehicleID string `json:"vehicle_id"`
}

func newFakeHedwigDataField() interface{} {
	return new(fakeHedwigDataField)
}

func (s *EncoderTestSuite) TestFormatHumanUUID() {
	testSchema := []byte(`{
    "id": "https://github.com/cloudchacho/hedwig-go/schema",
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
	encoder, err := NewEncoderDecoderFromBytes(testSchema, hedwig.DataFactoryRegistry{hedwig.MessageTypeMajorVersion{"vehicle_created", 1}: newString})
	s.NoError(err)

	data := json.RawMessage(`"6cac5588-24cc-4b4f-bbf9-7dc0ce93f96e"`)

	decoded, err := encoder.DecodeData(
		"vehicle_created",
		semver.MustParse("1.0"),
		data,
	)
	s.NoError(err)
	s.Equal("6cac5588-24cc-4b4f-bbf9-7dc0ce93f96e", *decoded.(*string))

	data = json.RawMessage(`"abcd"`)
	_, err = encoder.DecodeData(
		"vehicle_created",
		semver.MustParse("1.0"),
		data,
	)
	s.Error(err)

	data = json.RawMessage(`"yyyyyyyy-tttt-416a-92ed-420e62b33eb5"`)
	_, err = encoder.DecodeData(
		"vehicle_created",
		semver.MustParse("1.0"),
		data,
	)
	s.Error(err)
}

func (s *EncoderTestSuite) TestInvalidXVersion() {
	testSchema := []byte(`{
    "id": "https://github.com/cloudchacho/hedwig-go/schema",
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
	_, err := NewEncoderDecoderFromBytes(testSchema, hedwig.DataFactoryRegistry{hedwig.MessageTypeMajorVersion{"vehicle_created", 1}: newString})
	s.EqualError(err, "Missing x-version from schema definition for vehicle_created")

	testSchema = []byte(`{
    "id": "https://github.com/cloudchacho/hedwig-go/schema",
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
	_, err = NewEncoderDecoderFromBytes(testSchema, hedwig.DataFactoryRegistry{hedwig.MessageTypeMajorVersion{"vehicle_created", 1}: newString})
	s.EqualError(err, "invalid value for x-version: foobar, must be semver")

	testSchema = []byte(`{
    "id": "https://github.com/cloudchacho/hedwig-go/schema",
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
	_, err = NewEncoderDecoderFromBytes(testSchema, hedwig.DataFactoryRegistry{hedwig.MessageTypeMajorVersion{"vehicle_created", 1}: newString})
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
	messageType := s.encoder.EncodeMessageType("vehicle_created", semver.MustParse("1.0"))
	s.Equal(messageType, "https://github.com/cloudchacho/hedwig-go/schema#/schemas/vehicle_created/1.0")
}

func (s *EncoderTestSuite) TestEncodeData() {
	data := fakeHedwigDataField{VehicleID: "C_123"}
	metaAttrs := hedwig.MetaAttributes{
		Timestamp:     time.Unix(1621550514, 0),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "123",
		Schema:        "https://github.com/cloudchacho/hedwig-go/schema#/schemas/vehicle_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	}
	payload, err := s.encoder.EncodeData(data, true, metaAttrs)
	s.NoError(err)
	s.Equal(string(payload), "{\"vehicle_id\":\"C_123\"}")
}

func (s *EncoderTestSuite) TestEncodeDataContainerized() {
	data := fakeHedwigDataField{VehicleID: "C_123"}
	metaAttrs := hedwig.MetaAttributes{
		Timestamp:     time.Unix(1621550514, 0),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "123",
		Schema:        "https://github.com/cloudchacho/hedwig-go/schema#/schemas/vehicle_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	}
	payload, err := s.encoder.EncodeData(data, false, metaAttrs)
	s.NoError(err)
	s.Equal(string(payload), "{\"format_version\":\"1.0\","+
		"\"schema\":\"https://github.com/cloudchacho/hedwig-go/schema#/schemas/vehicle_created/1.0\","+
		"\"id\":\"123\","+
		"\"metadata\":{\"Timestamp\":1621550514000,\"Publisher\":\"myapp\",\"Headers\":{\"foo\":\"bar\"}},"+
		"\"data\":{\"vehicle_id\":\"C_123\"}}")
}

func (s *EncoderTestSuite) TestEncodeDataFailInvalidData() {
	data := make(chan bool)
	metaAttrs := hedwig.MetaAttributes{
		Timestamp:     time.Unix(1621550514, 0),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "123",
		Schema:        "https://github.com/cloudchacho/hedwig-go/schema#/schemas/vehicle_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	}
	_, err := s.encoder.EncodeData(data, true, metaAttrs)
	s.Error(err)
}

func (s *EncoderTestSuite) TestExtractData() {
	payload := []byte(`{"format_version":"1.0",
		"schema":"https://github.com/cloudchacho/hedwig-go/schema#/schemas/vehicle_created/1.0",
		"id":"d70a641e-14ab-32e4-a790-459bd36de532",
		"metadata":{"Timestamp":1621550514000,"Publisher":"myapp","Headers":{"foo":"bar"}},
		"data":{"vehicle_id":"C_123"}}`)
	attributes := map[string]string{
		"foo": "bar",
	}
	metaAttrs, extractedData, err := s.encoder.ExtractData(payload, attributes)
	s.NoError(err)
	s.Equal(metaAttrs, hedwig.MetaAttributes{
		Timestamp:     time.Unix(1621550514, 0),
		Publisher:     "myapp",
		Headers:       map[string]string{"foo": "bar"},
		ID:            "d70a641e-14ab-32e4-a790-459bd36de532",
		Schema:        "https://github.com/cloudchacho/hedwig-go/schema#/schemas/vehicle_created/1.0",
		FormatVersion: semver.MustParse("1.0"),
	})
	s.Equal(string(extractedData.(json.RawMessage)), "{\"vehicle_id\":\"C_123\"}")
}

func (s *EncoderTestSuite) TestExtractDataInvalid() {
	payload := []byte(`{"schema":"https://github.com/cloudchacho/hedwig-go/schema#/schemas/vehicle_created/1.0",
		"id":"d70a641e-14ab-32e4-a790-459bd36de532",
		"metadata":{"Timestamp":1621550514000,"Publisher":"myapp","Headers":{"foo":"bar"}},
		"data":{"vehicle_id":"C_123"}}`)
	attributes := map[string]string{
		"foo": "bar",
	}
	_, _, err := s.encoder.ExtractData(payload, attributes)
	s.Error(err)
}

func (s *EncoderTestSuite) TestDecodeMessageType() {
	schema := "https://github.com/cloudchacho/hedwig-go/schema#/schemas/vehicle_created/1.0"
	messageType, version, err := s.encoder.DecodeMessageType(schema)
	s.NoError(err)
	s.Equal(messageType, "vehicle_created")
	s.Equal(version, semver.MustParse("1.0"))

	schema = "vehicle_created 1.0"
	_, _, err = s.encoder.DecodeMessageType(schema)
	s.Error(err)
}

func (s *EncoderTestSuite) TestDecodeData() {
	data := json.RawMessage(`{"vehicle_id":"C_1234567890123456"}`)
	messageType := "vehicle_created"
	version := semver.MustParse("1.0")
	decodedData, err := s.encoder.DecodeData(messageType, version, data)
	s.NoError(err)
	s.Equal(decodedData, &fakeHedwigDataField{VehicleID: "C_1234567890123456"})
}

func (s *EncoderTestSuite) TestDecodeDataUnknownType() {
	data := json.RawMessage(`{"vehicle_id":"C_1234567890123456"}`)
	messageType := "unknown"
	version := semver.MustParse("1.0")
	_, err := s.encoder.DecodeData(messageType, version, data)
	s.Error(err)
}

func (s *EncoderTestSuite) TestDecodeDataUnknownVersion() {
	data := json.RawMessage(`{"vehicle_id":"C_1234567890123456"}`)
	messageType := "vehicle_created"
	version := semver.MustParse("2.0")
	_, err := s.encoder.DecodeData(messageType, version, data)
	s.Error(err)
}

func (s *EncoderTestSuite) TestDecodeDataInvalidSchema() {
	data := json.RawMessage(`{"vehicle_id":"C_1234567890123456"}`)
	messageType := "vehicle_created"
	version := semver.MustParse("2.0")
	_, err := s.encoder.DecodeData(messageType, version, data)
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
	version := semver.MustParse("2.0")
	_, err := s.encoder.DecodeData(messageType, version, data)
	s.Error(err)
}

func (s *EncoderTestSuite) TestDecodeDataInvalidDataJSON() {
	data := json.RawMessage(`{`)
	messageType := "vehicle_created"
	version := semver.MustParse("1.0")
	_, err := s.encoder.DecodeData(messageType, version, data)
	s.Error(err)
}

func (s *EncoderTestSuite) TestDecodeDataInvalidDataFactory() {
	data := json.RawMessage(`{}`)
	messageType := "trip_created"
	version := semver.MustParse("2.0")
	_, err := s.encoder.DecodeData(messageType, version, data)
	s.Error(err)
}

func (s *EncoderTestSuite) TestNew() {
	assert.NotNil(s.T(), s.encoder)
}

type EncoderTestSuite struct {
	suite.Suite
	encoder *EncoderDecoder
}

func (s *EncoderTestSuite) SetupTest() {
	registry := hedwig.DataFactoryRegistry{hedwig.MessageTypeMajorVersion{"vehicle_created", 1}: newFakeHedwigDataField}
	encoder, err := NewMessageEncoder("schema.json", registry)
	require.NoError(s.T(), err)

	s.encoder = encoder.(*EncoderDecoder)
}

func TestEncoderTestSuite(t *testing.T) {
	suite.Run(t, &EncoderTestSuite{})
}

func TestInvalidSchemaNotMajorVersion(t *testing.T) {
	assertions := assert.New(t)
	invalidSchema := `
	{
		"id": "https://github.com/cloudchacho/hedwig-go/schema",
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
						"vehicle_id": {gcp/gcp_test.go:26
							"type": "string"
						},
						"user_id": {
							"type": "string"
						},
						"vin": {
							"type": "string"
						}
					},
					"x-version": "1.0"
				}
			}
		}
	}
	`
	v, err := NewEncoderDecoderFromBytes([]byte(invalidSchema), hedwig.DataFactoryRegistry{})
	assertions.Nil(v)
	assertions.Error(err)
}

func TestInvalidSchemaMajorVersionMismatch(t *testing.T) {
	assertions := assert.New(t)
	invalidSchema := `
	{
		"id": "https://github.com/cloudchacho/hedwig-go/schema",
		"$schema": "http://json-schema.org/draft-04/schema#",
		"description": "Test Schema for Hedwig messages",
		"schemas": {
			"trip_created": {
				"1.*": {
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
					},
					"x-version": "2.0"
				}
			}
		}
	}
	`
	v, err := NewEncoderDecoderFromBytes([]byte(invalidSchema), hedwig.DataFactoryRegistry{})
	assertions.Nil(v)
	assertions.Error(err)
}

func TestInvalidSchemaNoXVersion(t *testing.T) {
	assertions := assert.New(t)
	invalidSchema := `
	{
		"id": "https://github.com/cloudchacho/hedwig-go/schema",
		"$schema": "http://json-schema.org/draft-04/schema#",
		"description": "Test Schema for Hedwig messages",
		"schemas": {
			"trip_created": {
				"1.*": {
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
	v, err := NewEncoderDecoderFromBytes([]byte(invalidSchema), hedwig.DataFactoryRegistry{})
	assertions.Nil(v)
	assertions.Error(err)
}

func TestInvalidSchemaNotJSON(t *testing.T) {
	assertions := assert.New(t)
	invalidSchema := `"https://github.com/cloudchacho/hedwig-go/schema"`
	v, err := NewEncoderDecoderFromBytes([]byte(invalidSchema), hedwig.DataFactoryRegistry{})
	assertions.Nil(v)
	assertions.Error(err)
}

func TestInvalidSchemaNotObject(t *testing.T) {
	assertions := assert.New(t)
	invalidSchema := `foobar`
	v, err := NewEncoderDecoderFromBytes([]byte(invalidSchema), hedwig.DataFactoryRegistry{})
	assertions.Nil(v)
	assertions.Error(err)
}

func TestInvalidSchemaMessageTypeNotFound(t *testing.T) {
	assertions := assert.New(t)
	schema := `
	{
		"id": "https://github.com/cloudchacho/hedwig-go/schema",
		"$schema": "http://json-schema.org/draft-04/schema#",
		"description": "Test Schema for Hedwig messages",
		"schemas": {
			"trip_created": {
				"1.*": {
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
					},
					"x-version": "1.0"
				}
			}
		}
	}
	`
	v, err := NewEncoderDecoderFromBytes([]byte(schema), hedwig.DataFactoryRegistry{{"user-created", 1}: newFakeHedwigDataField})
	assertions.Nil(v)
	assertions.EqualError(err, "Schema not found for message type user-created, major version 1")
}
