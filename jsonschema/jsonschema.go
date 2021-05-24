/*
 * Author: Michael Ngo
 */

package jsonschema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
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

var schemaKeyRegex *regexp.Regexp

const xVersionKey = "x-version"

var containerSchema *jsonschema.Schema

func init() {
	schemaKeyRegex = regexp.MustCompile(`([^/]+)/(\d+)\.(\d+)$`)

	addJSONSchemaCustomFormats()

	containerSchema = readContainerSchema()
}

func readContainerSchema() *jsonschema.Schema {
	compiler := jsonschema.NewCompiler()

	// Force to draft version 4
	compiler.Draft = jsonschema.Draft4

	err := compiler.AddResource("https://hedwig.automatic.com/format_schema", strings.NewReader(containerSchemaStr))
	if err != nil {
		fmt.Println(err)
		panic("unable to add schema resource - should never happen")
	}
	schema, err := compiler.Compile("https://hedwig.automatic.com/format_schema")
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

// NewEncoderFromBytes from an byte encoded schema file
func NewEncoderFromBytes(schemaFile []byte, dataRegistry hedwig.DataFactoryRegistry) (hedwig.IEncoder, error) {
	encoder := messageEncoder{
		compiledSchemaMap: make(map[string]*jsonschema.Schema),
		dataRegistry:      dataRegistry,
	}

	var parsedSchema map[string]interface{}
	err := json.Unmarshal(schemaFile, &parsedSchema)
	if err != nil {
		return nil, err
	}

	// Extract base url from schema id
	encoder.schemaID = parsedSchema["id"].(string)

	schemaMap := parsedSchema["schemas"].(map[string]interface{})
	for schemaName, schemaVersionObj := range schemaMap {
		schemaVersionMap := schemaVersionObj.(map[string]interface{})
		for version, schema := range schemaVersionMap {
			schemaByte, err := json.Marshal(schema)
			if err != nil {
				// should never happen, schema was already unmarshaled once
				return nil, err
			}

			compiler := jsonschema.NewCompiler()

			// Force to draft version 4
			compiler.Draft = jsonschema.Draft4

			compiler.Extensions["x-version"] = xVersionsExt()

			schemaURL := fmt.Sprintf("%s/schemas/%s/%s", encoder.schemaID, schemaName, version)

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

			if _, ok := schema.Extensions[xVersionKey]; !ok {
				return nil, errors.New("Missing x-version from schema definition")
			}

			schemaKey := fmt.Sprintf("%s/%s", schemaName, version)
			encoder.compiledSchemaMap[schemaKey] = schema
		}
	}

	return &encoder, nil
}

// NewMessageEncoder creates a new encoder from the given file
func NewMessageEncoder(schemaFilePath string, dataRegistry hedwig.DataFactoryRegistry) (hedwig.IEncoder, error) {
	rawSchema, err := ioutil.ReadFile(schemaFilePath)
	if err != nil {
		return nil, err
	}

	return NewEncoderFromBytes(rawSchema, dataRegistry)
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

// messageEncoder is an implementation of hedwig.IEncoder
type messageEncoder struct {
	// Format: schemakey("schema name/schema major version") => schema
	//   parking.created/3 => schema
	compiledSchemaMap map[string]*jsonschema.Schema

	dataRegistry hedwig.DataFactoryRegistry

	schemaID string
}

func (me *messageEncoder) schemaRoot() string {
	return me.schemaID
}

// EncodePayload encodes the message with appropriate format for transport over the wire
func (me *messageEncoder) EncodePayload(data interface{}, useMessageTransport bool, metaAttrs hedwig.MetaAttributes) ([]byte, map[string]string, error) {
	var payload []byte
	var msgAttrs map[string]string
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
			return nil, nil, err
		}
		msgAttrs = metaAttrs.Headers
	} else {
		payload, err = json.Marshal(data)
		if err != nil {
			// Unable to convert to JSON
			return nil, nil, err
		}
		msgAttrs = map[string]string{
			"hedwig_format_version":    fmt.Sprintf("%d.%d", metaAttrs.FormatVersion.Major(), metaAttrs.FormatVersion.Minor()),
			"hedwig_id":                metaAttrs.ID,
			"hedwig_message_timestamp": fmt.Sprintf("%d", metaAttrs.Timestamp.Unix()),
			"hedwig_publisher":         metaAttrs.Publisher,
			"hedwig_schema":            metaAttrs.Schema,
		}
		for k, v := range metaAttrs.Headers {
			msgAttrs[k] = v
		}
	}
	return payload, msgAttrs, nil
}

// VerifyKnownMinorVersion checks that message version is known to us
func (me *messageEncoder) VerifyKnownMinorVersion(messageType string, version *semver.Version) error {
	schemaKey := fmt.Sprintf("%s/%d.*", messageType, version.Major())

	if schema, ok := me.compiledSchemaMap[schemaKey]; ok {
		schemaVersion := schema.Extensions[xVersionKey].(*semver.Version)
		if schemaVersion.LessThan(version) {
			return errors.Errorf("Unknown minor version: {%d}, last known minor version: %d",
				version.Minor(), schemaVersion.Minor())
		}
		return nil
	}
	return errors.Errorf("No schema found for %s", schemaKey)
}

// EncodeMessageType encodes the message type with appropriate format for transport over the wire
func (me *messageEncoder) EncodeMessageType(messageType string, version *semver.Version) string {
	return fmt.Sprintf("%s#/schemas/%s/%d.%d", me.schemaRoot(), messageType, version.Major(), version.Minor())
}

// DecodeMessageType decodes message type from meta attributes
func (me *messageEncoder) DecodeMessageType(schema string) (string, *semver.Version, error) {
	m := schemaKeyRegex.FindStringSubmatch(schema)
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

// ExtractData extracts data from the on-the-wire payload
func (me *messageEncoder) ExtractData(messagePayload []byte, attributes map[string]string, useMessageTransport bool) (hedwig.MetaAttributes, interface{}, error) {
	metaAttrs := hedwig.MetaAttributes{}

	if me.dataRegistry == nil {
		return metaAttrs, nil, errors.New("dataRegistry must be set")
	}

	if !useMessageTransport {
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
	} else {
		if value, ok := attributes["hedwig_format_version"]; !ok {
			return metaAttrs, nil, errors.New("value not found for attribute: 'hedwig_format_version'")
		} else {
			if version, err := semver.NewVersion(value); err != nil {
				return metaAttrs, nil, errors.Errorf("invalid value '%s' found for attribute: 'hedwig_format_version'", value)
			} else {
				metaAttrs.FormatVersion = version
			}
		}
		if value, ok := attributes["hedwig_id"]; !ok {
			return metaAttrs, nil, errors.New("value not found for attribute: 'hedwig_id'")
		} else {
			metaAttrs.ID = value
		}
		if value, ok := attributes["hedwig_message_timestamp"]; !ok {
			return metaAttrs, nil, errors.New("value not found for attribute: 'hedwig_id'")
		} else {
			if timestamp, err := strconv.ParseInt(value, 10, 64); err != nil {
				return metaAttrs, nil, errors.Errorf("invalid value '%s' found for attribute: 'hedwig_message_timestamp'", value)
			} else {
				unixTime := time.Unix(timestamp/1000.0, 0)
				metaAttrs.Timestamp = unixTime
			}
		}
		if value, ok := attributes["hedwig_publisher"]; !ok {
			return metaAttrs, nil, errors.New("value not found for attribute: 'hedwig_publisher'")
		} else {
			metaAttrs.Publisher = value
		}
		if value, ok := attributes["hedwig_schema"]; !ok {
			return metaAttrs, nil, errors.New("value not found for attribute: 'hedwig_schema'")
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

		return metaAttrs, json.RawMessage(messagePayload), nil
	}
}

// DecodeData validates and decodes data
func (me *messageEncoder) DecodeData(metaAttrs hedwig.MetaAttributes, messageType string, version *semver.Version, data interface{}) (interface{}, error) {
	if !strings.HasPrefix(metaAttrs.Schema, me.schemaRoot()) {
		return nil, errors.Errorf("Message schema must start with %s", me.schemaRoot())
	}

	var dataTyped []byte
	var ok bool

	if dataTyped, ok = data.(json.RawMessage); !ok {
		return nil, errors.Errorf("Unexpected data of type: %s, expected json.RawMessage", reflect.TypeOf(data))
	}

	schemaKey := fmt.Sprintf("%s/%d.*", messageType, version.Major())

	var schema *jsonschema.Schema

	if schema, ok = me.compiledSchemaMap[schemaKey]; !ok {
		return nil, errors.Errorf("Unknown schema: %s", metaAttrs.Schema)
	}

	var dataFactory hedwig.DataFactory
	if dataFactory, ok = me.dataRegistry[hedwig.DataRegistryKey{
		MessageType:         messageType,
		MessageMajorVersion: int(version.Major()),
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
