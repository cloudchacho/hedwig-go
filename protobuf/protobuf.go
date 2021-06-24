package protobuf

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"

	"github.com/Masterminds/semver"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/cloudchacho/hedwig-go"
)

var schemaRegex *regexp.Regexp

var messageNameRegex *regexp.Regexp

func init() {
	schemaRegex = regexp.MustCompile(`([^/]+)/(\d+)\.(\d+)$`)

	messageNameRegex = regexp.MustCompile(`^(.*)V(\d+)$`)
}

// messageEncoder is an implementation of hedwig.IEncoder
type messageEncoder struct {
	protoMsgs map[hedwig.MessageTypeMajorVersion]protoreflect.Message
	versions  map[hedwig.MessageTypeMajorVersion]*semver.Version
}

// NewMessageEncoderFromMessageTypes creates a new encoder from explicit message types mapping
func NewMessageEncoderFromMessageTypes(protoMessages map[hedwig.MessageTypeMajorVersion]protoreflect.Message) (hedwig.IEncoder, error) {
	protoMessagesMap := map[hedwig.MessageTypeMajorVersion]protoreflect.Message{}
	versions := map[hedwig.MessageTypeMajorVersion]*semver.Version{}
	for messageTypeMajorVersion, msg := range protoMessages {
		if messageTypeMajorVersion.MessageType == "" {
			return nil, errors.New("invalid message type: must not be empty")
		}
		if messageTypeMajorVersion.MajorVersion == 0 {
			return nil, errors.New("invalid major version: must not be zero")
		}
		desc := msg.Descriptor()
		options := desc.Options().(*descriptorpb.MessageOptions)
		hedwigMsgOpts := proto.GetExtension(options, E_MessageOptions).(*MessageOptions)
		msgOptsMajorVersion := uint(hedwigMsgOpts.GetMajorVersion())
		minorVersion := int(hedwigMsgOpts.GetMinorVersion())
		if hedwigMsgOpts.GetMessageType() != "" && messageTypeMajorVersion.MessageType != hedwigMsgOpts.GetMessageType() {
			return nil, errors.Errorf("message type mismatch from message_options for %v", messageTypeMajorVersion)
		}
		if messageTypeMajorVersion.MajorVersion != msgOptsMajorVersion {
			return nil, errors.Errorf("invalid major version in message_options: %d for %s", msgOptsMajorVersion, desc.Name())
		}
		version, err := semver.NewVersion(fmt.Sprintf("%d.%d", messageTypeMajorVersion.MajorVersion, minorVersion))
		if err != nil {
			// will never happen, version was constructed by string formatting
			return nil, err
		}
		protoMessagesMap[messageTypeMajorVersion] = msg
		versions[messageTypeMajorVersion] = version
	}
	return &messageEncoder{protoMsgs: protoMessagesMap, versions: versions}, nil
}

// NewMessageEncoder creates a new encoder from given list of proto-messages
// Proto messages must declare [hedwig.message_options](https://github.com/cloudchacho/hedwig/blob/main/protobuf/options.proto) option.
// See [example proto file](../examples/schema.proto) for reference.
//
// This method will try to read message type from message_options, and if not specified,
// assume that the messages are named as: `<MessageType>V<MajorVersion>`. If that doesn't work
// for your use case, use NewMessageEncoderFromMessageTypes and provide an explicit mapping.
func NewMessageEncoder(protoMessages []proto.Message) (hedwig.IEncoder, error) {
	protoMessagesMap := map[hedwig.MessageTypeMajorVersion]protoreflect.Message{}
	versions := map[hedwig.MessageTypeMajorVersion]*semver.Version{}
	for _, msg := range protoMessages {
		desc := msg.ProtoReflect().Descriptor()
		var messageType string
		var majorVersion uint
		if matches := messageNameRegex.FindStringSubmatch(string(desc.Name())); len(matches) > 0 {
			messageType = matches[1]
			var err error
			var majorVersionSigned int
			if majorVersionSigned, err = strconv.Atoi(matches[2]); err != nil {
				// will never happen, message name already passed regex
				return nil, err
			}
			majorVersion = uint(majorVersionSigned)
		}
		options := desc.Options().(*descriptorpb.MessageOptions)
		hedwigMsgOpts := proto.GetExtension(options, E_MessageOptions).(*MessageOptions)
		msgOptsMajorVersion := uint(hedwigMsgOpts.GetMajorVersion())
		if msgOptsMajorVersion != 0 {
			if majorVersion == 0 {
				majorVersion = msgOptsMajorVersion
			} else if majorVersion != msgOptsMajorVersion {
				return nil, errors.Errorf("invalid major version in message_options: %d for %s", msgOptsMajorVersion, desc.Name())
			}
		} else if majorVersion == 0 {
			return nil, errors.Errorf("invalid proto message, can't determine major version %s", desc.Name())
		}
		minorVersion := int(hedwigMsgOpts.GetMinorVersion())
		if hedwigMsgOpts.GetMessageType() != "" {
			messageType = hedwigMsgOpts.GetMessageType()
		} else if messageType == "" {
			return nil, errors.Errorf("invalid proto message, can't determine message type %s", desc.Name())
		}
		version, err := semver.NewVersion(fmt.Sprintf("%d.%d", majorVersion, minorVersion))
		if err != nil {
			// will never happen, version was constructed by string formatting
			return nil, err
		}
		schemaKey := hedwig.MessageTypeMajorVersion{messageType, majorVersion}
		if _, ok := protoMessagesMap[schemaKey]; ok {
			return nil, errors.Errorf("duplicate message found for %s %d", messageType, majorVersion)
		}
		protoMessagesMap[schemaKey] = msg.ProtoReflect()
		versions[schemaKey] = version
	}
	return &messageEncoder{protoMsgs: protoMessagesMap, versions: versions}, nil
}

// EncodeData encodes the message with appropriate format for transport over the wire
// Type of data must be proto.Message
func (me *messageEncoder) EncodeData(data interface{}, useMessageTransport bool, metaAttrs hedwig.MetaAttributes) ([]byte, error) {
	var payload []byte
	var ok bool
	var dataTyped proto.Message
	if dataTyped, ok = data.(proto.Message); !ok {
		return nil, errors.Errorf("Unexpected data of type: %s, expected *proto.Message", reflect.TypeOf(data))
	}

	if !useMessageTransport {
		anyMsg, err := anypb.New(dataTyped)
		if err != nil {
			// Unable to convert to bytes
			return nil, err
		}
		container := &PayloadV1{
			FormatVersion: fmt.Sprintf("%d.%d", metaAttrs.FormatVersion.Major(), metaAttrs.FormatVersion.Minor()),
			Id:            metaAttrs.ID,
			Metadata: &MetadataV1{
				Publisher: metaAttrs.Publisher,
				Timestamp: timestamppb.New(metaAttrs.Timestamp),
				Headers:   metaAttrs.Headers,
			},
			Schema: metaAttrs.Schema,
			Data:   anyMsg,
		}
		payload, err = proto.Marshal(container)
		if err != nil {
			// Unable to convert to bytes
			return nil, err
		}
	} else {
		var err error
		payload, err = proto.Marshal(dataTyped)
		if err != nil {
			// Unable to convert to bytes
			return nil, err
		}
	}
	return payload, nil
}

// VerifyKnownMinorVersion checks that message version is known to us
func (me *messageEncoder) VerifyKnownMinorVersion(messageType string, version *semver.Version) error {
	protoMessageKey := hedwig.MessageTypeMajorVersion{messageType, uint(version.Major())}

	if schemaVersion, ok := me.versions[protoMessageKey]; ok {
		if schemaVersion.LessThan(version) {
			return errors.Errorf("Unknown minor version: {%d}, last known minor version: %d",
				version.Minor(), schemaVersion.Minor())
		}
		return nil
	}
	return errors.Errorf("No protoMsg found for %v", protoMessageKey)
}

// EncodeMessageType encodes the message type with appropriate format for transport over the wire
func (me *messageEncoder) EncodeMessageType(messageType string, version *semver.Version) string {
	return fmt.Sprintf("%s/%d.%d", messageType, version.Major(), version.Minor())
}

// DecodeMessageType decodes message type from meta attributes
func (me *messageEncoder) DecodeMessageType(schema string) (string, *semver.Version, error) {
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

// ExtractData extracts data from the on-the-wire payload
// Type of data will be *anypb.Any
func (me *messageEncoder) ExtractData(messagePayload []byte, attributes map[string]string) (hedwig.MetaAttributes, interface{}, error) {
	metaAttrs := hedwig.MetaAttributes{}
	payload := PayloadV1{}
	err := proto.Unmarshal(messagePayload, &payload)
	if err != nil {
		return metaAttrs, nil, errors.Wrap(err, "Unexpected data couldn't be unmarshaled")
	}
	formatVersion, err := semver.NewVersion(payload.FormatVersion)
	if err != nil {
		return metaAttrs, nil, errors.Wrap(err, "Unexpected data: invalid format version")
	}

	metaAttrs.Timestamp = payload.Metadata.Timestamp.AsTime()
	metaAttrs.Publisher = payload.Metadata.Publisher
	metaAttrs.Headers = payload.Metadata.Headers
	metaAttrs.ID = payload.Id
	metaAttrs.Schema = payload.Schema
	metaAttrs.FormatVersion = formatVersion

	return metaAttrs, payload.Data, nil
}

// DecodeData validates and decodes data
// Type of data must be *anypb.Any for containerized format or []byte for non-containerized format
func (me *messageEncoder) DecodeData(messageType string, version *semver.Version, data interface{}) (interface{}, error) {
	var ok bool
	schemaKey := hedwig.MessageTypeMajorVersion{messageType, uint(version.Major())}
	msgClass, ok := me.protoMsgs[schemaKey]
	if !ok {
		return nil, errors.Errorf("Proto message not found for: %s %d.%d", messageType, version.Major(), version.Minor())
	}
	msg := msgClass.New().Interface()
	var err error

	if dataTyped, ok := data.(*anypb.Any); ok {
		// containerized format
		err = dataTyped.UnmarshalTo(msg)
	} else if dataTyped, ok := data.([]byte); ok {
		err = proto.Unmarshal(dataTyped, msg)
	} else {
		return nil, errors.Errorf("Unexpected data of type %s, expected *anypb.Any or []byte", reflect.TypeOf(data))
	}
	if err != nil {
		return nil, errors.Wrap(err, "Unexpected data couldn't be unmarshaled")
	}
	return msg, nil
}
