/*
 * Author: Michael Ngo
 */

package hedwig

import (
	"context"
	"testing"

	"github.com/pkg/errors"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func (s *PublisherTestSuite) TestPublish() {
	ctx := context.Background()

	data := fakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	message, err := NewMessage(s.settings, "user-created", "1.0", nil, &data)
	s.Require().NoError(err)

	payload := []byte(`{"type": "user-created"}`)
	headers := map[string]string{}

	s.validator.On("Serialize", message).
		Return(payload, headers, nil)

	messageId := "123"

	s.backend.On("Publish", ctx, message, payload, headers, "dev-user-created-v1").
		Return(messageId, nil)

	receivedMessageId, err := s.publisher.Publish(ctx, message)
	s.Nil(err)
	s.Equal(messageId, receivedMessageId)

	s.backend.AssertExpectations(s.T())
}

func (s *PublisherTestSuite) TestPublishTopicError() {
	ctx := context.Background()

	data := fakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	message, err := NewMessage(s.settings, "user-created", "2.0", nil, &data)
	s.Require().NoError(err)

	payload := []byte(`{"type": "user-created"}`)
	headers := map[string]string{}

	s.validator.On("Serialize", message).
		Return(payload, headers, nil)

	_, err = s.publisher.Publish(ctx, message)
	s.EqualError(err, "Message route is not defined for message")

	s.backend.AssertExpectations(s.T())
}

func (s *PublisherTestSuite) TestPublishSerializeError() {
	ctx := context.Background()

	data := fakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	message, err := NewMessage(s.settings, "user-created", "2.0", nil, &data)
	s.Require().NoError(err)

	s.validator.On("Serialize", message).
		Return([]byte(""), map[string]string{}, errors.New("failed to serialize"))

	_, err = s.publisher.Publish(ctx, message)
	s.EqualError(err, "failed to serialize")

	s.backend.AssertExpectations(s.T())
}

func (s *PublisherTestSuite) TestNew() {
	assert.NotNil(s.T(), s.publisher)
}

type PublisherTestSuite struct {
	suite.Suite
	publisher *Publisher
	backend   *fakeBackend
	validator *fakeValidator
	settings  *Settings
}

func (s *PublisherTestSuite) SetupTest() {
	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		QueueName:    "dev-myapp",
		MessageRouting: map[MessageTypeMajorVersion]string{
			{
				MessageType:  "user-created",
				MajorVersion: 1,
			}: "dev-user-created-v1",
		},
	}
	backend := &fakeBackend{}
	validator := &fakeValidator{}

	s.publisher = NewPublisher(settings, backend, validator).(*Publisher)
	s.backend = backend
	s.validator = validator
	s.settings = settings
}

func TestPublisherTestSuite(t *testing.T) {
	suite.Run(t, &PublisherTestSuite{})
}
