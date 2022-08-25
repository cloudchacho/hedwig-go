package jsonschema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/Masterminds/semver"
	"github.com/pkg/errors"
	"github.com/santhosh-tekuri/jsonschema/v3"

	"github.com/cloudchacho/hedwig-go"
)

var schemaRegex *regexp.Regexp

var schemaMajorVersionRegexp *regexp.Regexp

const xVersionKey = "x-version"

var containerSchema *jsonschema.Schema

func init() {
	schemaRegex = regexp.MustCompile(`([^/]+)/(\d+)\.(\d+)$`)

	schemaMajorVersionRegexp = regexp.MustCompile(`^(\d+)\.\*$`)

	addJSONSchemaCustomFormats()

	containerSchema = readContainerSchema()
}

func readContainerSchema() *jsonschema.Schema {
	compiler := jsonschema.NewCompiler()

	// Force to draft version 4
	compiler.Draft = jsonschema.Draft4

	err := compiler.AddResource("https://github.com/cloudchacho/hedwig-go/schemas/format_schema", strings.NewReader(containerSchemaStr))
	if err != nil {
		fmt.Println(err)
		panic("unable to add schema resource - should never happen")
	}
	schema, err := compiler.Compile("https://github.com/cloudchacho/hedwig-go/schemas/format_schema")
	if err != nil {
		fmt.Println(err)
		panic("unable to compile schema - should never happen")
	}
	return schema
}

// Add custom JSON schema formats
func addJSONSchemaCustomFormats() {
	humanUUIDRegex := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

	// Validates this is a human readable uuid (uuid separated by hyphens)
	jsonschema.Formats["human-uuid"] = func(in interface{}) bool {
		s, ok := in.(string)
		if !ok {
			return true
		}
		return humanUUIDRegex.MatchString(s)
	}
}

func xVersionsExt() jsonschema.Extension {
	meta, err := jsonschema.CompileString("xVersions.json", `{
		"properties" : {
			"x-version": {
				"type": "string"
			}
		}
	}`)
	if err != nil {
		panic(err)
	}
	compile := func(ctx jsonschema.CompilerContext, m map[string]interface{}) (interface{}, error) {
		if xVersion, ok := m["x-version"]; ok {
			if xVersionStr, ok := xVersion.(string); ok {
				version, err := semver.NewVersion(xVersionStr)
				if err != nil {
					return nil, errors.Errorf("invalid value for x-version: %s, must be semver", xVersion)
				}
				return version, err
			}
			// should never happen since value is validated to be a string already
			return nil, errors.Errorf("invalid value for x-version: %s", xVersion)
		}
		return nil, nil
	}
	validate := func(ctx jsonschema.ValidationContext, s interface{}, v interface{}) error {
		return nil
	}
	return jsonschema.Extension{
		Meta:     meta,
		Compile:  compile,
		Validate: validate,
	}
}

// NewEncoderDecoderFromBytes creates an encoder / decoder from a byte encoded JSON schema file
func NewEncoderDecoderFromBytes(schemaFile []byte, dataRegistry DataFactoryRegistry) (*EncoderDecoder, error) {
	encoder := EncoderDecoder{
		compiledSchemaMap: make(map[hedwig.MessageTypeMajorVersion]*jsonschema.Schema),
		dataRegistry:      dataRegistry,
	}

	var parsedSchema map[string]interface{}
	err := json.Unmarshal(schemaFile, &parsedSchema)
	if err != nil {
		return nil, err
	}

	// Extract base url from schema id
	encoder.schemaID = parsedSchema["id"].(string)

	msgTypesFound := map[hedwig.MessageTypeMajorVersion]bool{}
	for messageMajor := range dataRegistry {
		msgTypesFound[messageMajor] = false
	}

	schemaMap := parsedSchema["schemas"].(map[string]interface{})
	for messageType, schemaVersionObj := range schemaMap {
		schemaVersionMap := schemaVersionObj.(map[string]interface{})
		for version, schema := range schemaVersionMap {
			matches := schemaMajorVersionRegexp.FindStringSubmatch(version)
			if matches == nil {
				return nil, errors.Errorf("invalid version %s for %s", version, messageType)
			}

			majorVersionSigned, err := strconv.Atoi(matches[1])
			if err != nil {
				// should never happen, regex already validated
				return nil, err
			}

			majorVersion := uint(majorVersionSigned)

			schemaByte, err := json.Marshal(schema)
			if err != nil {
				// should never happen, schema was already unmarshaled once
				return nil, err
			}

			compiler := jsonschema.NewCompiler()

			// Force to draft version 4
			compiler.Draft = jsonschema.Draft4

			compiler.Extensions["x-version"] = xVersionsExt()

			schemaURL := fmt.Sprintf("%s/schemas/%s/%s", encoder.schemaID, messageType, version)

			err = compiler.AddResource(schemaURL, strings.NewReader(string(schemaByte)))
			if err != nil {
				// should never happen, the schema bytes were already marshaled
				return nil, err
			}

			err = compiler.AddResource(encoder.schemaID, strings.NewReader(string(schemaFile)))
			if err != nil {
				// should never happen, schema was already unmarshaled once
				return nil, err
			}

			schema, err := compiler.Compile(schemaURL)
			if err != nil {
				return nil, err
			}

			var value interface{}
			var ok bool

			if value, ok = schema.Extensions[xVersionKey]; !ok {
				return nil, errors.Errorf("Missing x-version from schema definition for %s", messageType)
			}
			xVersion := value.(*semver.Version)
			if xVersion.Major() != int64(majorVersion) {
				return nil, errors.Errorf("Invalid x-version: %d.%d for: %s/%s",
					xVersion.Major(), xVersion.Minor(), messageType, version,
				)
			}

			schemaKey := hedwig.MessageTypeMajorVersion{messageType, majorVersion}
			encoder.compiledSchemaMap[schemaKey] = schema

			msgTypesFound[schemaKey] = true
		}
	}

	for messageMajor, found := range msgTypesFound {
		if !found {
			return nil, errors.Errorf("Schema not found for message type %s, major version %d", messageMajor.MessageType, messageMajor.MajorVersion)
		}
	}

	return &encoder, nil
}

// NewMessageEncoderDecoder creates a new encoder from the given file
func NewMessageEncoderDecoder(schemaFilePath string, dataRegistry DataFactoryRegistry) (*EncoderDecoder, error) {
	rawSchema, err := os.ReadFile(schemaFilePath)
	if err != nil {
		return nil, err
	}

	return NewEncoderDecoderFromBytes(rawSchema, dataRegistry)
}

type messageContainerMetadata struct {
	Timestamp JSONTime          `json:"Timestamp"`
	Publisher string            `json:"Publisher"`
	Headers   map[string]string `json:"Headers,omitempty"`
}

type messageContainer struct {
	FormatVersion string                   `json:"format_version"`
	Schema        string                   `json:"schema"`
	ID            string                   `json:"id"`
	Metadata      messageContainerMetadata `json:"metadata"`
	Data          interface{}              `json:"data"`
}

type messageDeserializationContainer struct {
	FormatVersion *semver.Version          `json:"format_version"`
	Schema        string                   `json:"schema"`
	ID            string                   `json:"id"`
	Metadata      messageContainerMetadata `json:"metadata"`
	Data          json.RawMessage          `json:"data"`
}

// EncoderDecoder is an implementation of hedwig.Encoder and hedwig.Decoder that uses JSON Schema
type EncoderDecoder struct {
	compiledSchemaMap map[hedwig.MessageTypeMajorVersion]*jsonschema.Schema

	dataRegistry DataFactoryRegistry

	schemaID string
}

var _ = hedwig.Encoder(&EncoderDecoder{})
var _ = hedwig.Decoder(&EncoderDecoder{})

func (ed *EncoderDecoder) schemaRoot() string {
	return ed.schemaID
}

// EncodeData encodes the message with appropriate format for transport over the wire
func (ed *EncoderDecoder) EncodeData(data interface{}, useMessageTransport bool, metaAttrs hedwig.MetaAttributes) ([]byte, error) {
	var payload []byte
	var err error

	if !useMessageTransport {
		payload, err = json.Marshal(messageContainer{
			FormatVersion: fmt.Sprintf("%d.%d", metaAttrs.FormatVersion.Major(), metaAttrs.FormatVersion.Minor()),
			Schema:        metaAttrs.Schema,
			ID:            metaAttrs.ID,
			Metadata: messageContainerMetadata{
				Timestamp: JSONTime(metaAttrs.Timestamp),
				Publisher: metaAttrs.Publisher,
				Headers:   metaAttrs.Headers,
			},
			Data: data,
		})
		if err != nil {
			// Unable to convert to JSON
			return nil, err
		}
	} else {
		payload, err = json.Marshal(data)
		if err != nil {
			// Unable to convert to JSON
			return nil, err
		}
	}
	return payload, nil
}

// VerifyKnownMinorVersion checks that message version is known to us
func (ed *EncoderDecoder) VerifyKnownMinorVersion(messageType string, version *semver.Version) error {
	schemaKey := hedwig.MessageTypeMajorVersion{messageType, uint(version.Major())}

	if schema, ok := ed.compiledSchemaMap[schemaKey]; ok {
		schemaVersion := schema.Extensions[xVersionKey].(*semver.Version)
		if schemaVersion.LessThan(version) {
			return errors.Errorf("Unknown minor version: {%d}, last known minor version: %d",
				version.Minor(), schemaVersion.Minor())
		}
		return nil
	}
	return errors.Errorf("No schema found for %v", schemaKey)
}

// EncodeMessageType encodes the message type with appropriate format for transport over the wire
func (ed *EncoderDecoder) EncodeMessageType(messageType string, version *semver.Version) string {
	return fmt.Sprintf("%s#/schemas/%s/%d.%d", ed.schemaRoot(), messageType, version.Major(), version.Minor())
}

func (ed *EncoderDecoder) IsBinary() bool {
	return false
}

// DecodeMessageType decodes message type from meta attributes
func (ed *EncoderDecoder) DecodeMessageType(schema string) (string, *semver.Version, error) {
	if !strings.HasPrefix(schema, ed.schemaRoot()) {
		return "", nil, errors.Errorf("Message schema must start with %s", ed.schemaRoot())
	}

	m := schemaRegex.FindStringSubmatch(schema)
	if len(m) == 0 {
		return "", nil, errors.Errorf("invalid schema: '%s' doesn't match valid regex", schema)
	}

	versionStr := fmt.Sprintf("%s.%s", m[2], m[3])
	version, err := semver.NewVersion(versionStr)
	if err != nil {
		// would never happen
		return "", nil, errors.Errorf("unable to parse as version: %s", versionStr)
	}
	return m[1], version, nil
}

// ExtractData extracts data from the on-the-wire payload when not using message transport
func (ed *EncoderDecoder) ExtractData(messagePayload []byte, attributes map[string]string) (hedwig.MetaAttributes, interface{}, error) {
	metaAttrs := hedwig.MetaAttributes{}

	err := containerSchema.Validate(bytes.NewReader(messagePayload))
	if err != nil {
		return metaAttrs, nil, errors.Wrap(err, "unable to verify containerized format")
	}

	container := messageDeserializationContainer{}
	err = json.Unmarshal(messagePayload, &container)
	if err != nil {
		// would never happen
		return metaAttrs, nil, err
	}
	metaAttrs.Timestamp = time.Time(container.Metadata.Timestamp)
	metaAttrs.Publisher = container.Metadata.Publisher
	metaAttrs.Headers = container.Metadata.Headers
	metaAttrs.ID = container.ID
	metaAttrs.Schema = container.Schema
	metaAttrs.FormatVersion = container.FormatVersion

	return metaAttrs, container.Data, nil
}

// DecodeData validates and decodes data
func (ed *EncoderDecoder) DecodeData(messageType string, version *semver.Version, data interface{}) (interface{}, error) {
	var dataTyped []byte

	if dataTypedRawMessage, ok := data.(json.RawMessage); ok {
		dataTyped = []byte(dataTypedRawMessage)
	} else if dataTyped, ok = data.([]byte); !ok {
		return nil, errors.Errorf("Unexpected data of type: %s, expected json.RawMessage or []byte", reflect.TypeOf(data))
	}

	schemaKey := hedwig.MessageTypeMajorVersion{messageType, uint(version.Major())}

	var schema *jsonschema.Schema
	var ok bool

	if schema, ok = ed.compiledSchemaMap[schemaKey]; !ok {
		return nil, errors.Errorf("Unknown schema: %v", schemaKey)
	}

	var dataFactory DataFactory
	if dataFactory, ok = ed.dataRegistry[hedwig.MessageTypeMajorVersion{
		MessageType:  messageType,
		MajorVersion: uint(version.Major()),
	}]; !ok {
		return nil, errors.Errorf("dataRegistry entry not found for: %s/%d", messageType, version.Major())
	}
	decoded := dataFactory()
	if err := json.Unmarshal(dataTyped, &decoded); err != nil {
		return nil, err
	}

	err := schema.Validate(bytes.NewReader(dataTyped))
	if err != nil {
		return nil, errors.Wrapf(err, "Unable to validate data")
	}
	return decoded, nil
}

// DataFactory is a function that returns a pointer to struct type that a hedwig message data should conform to
type DataFactory func() interface{}

// DataFactoryRegistry is the map of message type and major versions to a factory function
type DataFactoryRegistry map[hedwig.MessageTypeMajorVersion]DataFactory
