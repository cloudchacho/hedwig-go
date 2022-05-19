package hedwig

import (
	"bytes"
	"encoding/binary"
	"io"
)

type Firehose struct {
	messageValidator *messageValidator
}

func (f *Firehose) Deserialize(reader io.Reader) ([]Message, error) {
	// TODO: loop through contents and return messages
	var messagePayload []byte
	runWithTransportMessageAttributes := false
	var messages []Message
	for {
		if f.messageValidator.encoder.IsBinary() {
			// TLV format: 8 bytes for size of message, n bytes for the actual message
			msgSize := make([]byte, 8)
			_, err := reader.Read(msgSize)
			if err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}
			msgLength := binary.LittleEndian.Uint64(msgSize)
			messagePayload = make([]byte, msgLength)
			_, err = reader.Read(messagePayload)
			if err != nil && err != io.EOF {
				return nil, err
			}
		} else {
			messagePayload = make([]byte, 10)
			// last char is new line, skip that
			// copy(messagePayload, line[:len(line)-1])
		}

		res, err := f.messageValidator.deserialize(messagePayload, nil, nil, &runWithTransportMessageAttributes)
		if err != nil {
			return nil, err
		}
		messages = append(messages, *res)
	}
	return messages, nil
}

func (f *Firehose) Serialize(message *Message) ([]byte, error) {
	runWithTransportMessageAttributes := false
	messagePayload, _, err := f.messageValidator.serialize(message, &runWithTransportMessageAttributes)
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
