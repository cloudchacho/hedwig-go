package hedwig

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type Firehose struct {
	messageValidator *messageValidator
}

func (f *Firehose) Deserialize(reader io.Reader) ([]Message, error) {
	overrideUseMsgAttrs := false
	var messages []Message
	if f.messageValidator.encoder.IsBinary() {
		var messagePayload []byte
		for {
			// TLV format: 8 bytes for size of message, n bytes for the actual message
			msgSize := make([]byte, 8)
			l, err := reader.Read(msgSize)
			if err == io.EOF && l != 0 {
				return nil, fmt.Errorf("TLV may be malformed EOF reached when trying to read length")
			} else if err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}
			msgLength := binary.LittleEndian.Uint64(msgSize)
			if len(messagePayload) < int(msgLength) {
				messagePayload = append(messagePayload, make([]byte, int(msgLength)-len(messagePayload))...)
			}
			_, err = io.ReadFull(reader, messagePayload[:msgLength])
			if err != nil && err != io.EOF {
				return nil, err
			}

			res, err := f.messageValidator.deserialize(messagePayload[:msgLength], nil, nil, &overrideUseMsgAttrs)
			if err != nil {
				return nil, err
			}
			messages = append(messages, *res)
		}
	} else {
		s := bufio.NewScanner(reader)
		s.Split(bufio.ScanLines)
		for s.Scan() {
			if err := s.Err(); err != nil {
				return nil, err
			}
			messagePayload := s.Bytes()

			res, err := f.messageValidator.deserialize(messagePayload, nil, nil, &overrideUseMsgAttrs)
			if err != nil {
				return nil, err
			}
			messages = append(messages, *res)
		}
	}

	return messages, nil
}

func (f *Firehose) Serialize(message *Message) ([]byte, error) {
	overrideUseMsgAttrs := false
	messagePayload, _, err := f.messageValidator.serialize(message, &overrideUseMsgAttrs)
	if err != nil {
		return nil, err
	}
	var encodedMessage []byte
	if f.messageValidator.encoder.IsBinary() {
		// TLV format: 8 bytes for size of message, n bytes for the actual message
		encodedMessage = make([]byte, 8+len(messagePayload))
		binary.LittleEndian.PutUint64(encodedMessage, uint64(len(messagePayload)))
		copy(encodedMessage[8:], messagePayload)
	} else {
		encodedMessage = make([]byte, 1+len(messagePayload))
		copy(encodedMessage, messagePayload)
		encodedMessage[len(encodedMessage)-1] = byte('\n')
	}
	r := bytes.NewReader(encodedMessage)
	_, err = f.Deserialize(r)
	return encodedMessage, err
}

func NewFirehose(encoder Encoder, decoder Decoder) *Firehose {
	v := newMessageValidator(encoder, decoder)
	f := Firehose{
		messageValidator: v,
	}
	return &f
}
