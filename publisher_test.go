/*
 * Copyright 2017, Automatic Inc.
 * All rights reserved.
 *
 * Author: Michael Ngo
 */

package hedwig

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/stretchr/testify/assert"
)

func (s *PublisherTestSuite) TestPublish() {
	assertions := s.Assertions

	ctx := context.Background()

	data := FakeHedwigDataField{
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
	assertions.Nil(err)
	assertions.Equal(messageId, receivedMessageId)

	s.backend.AssertExpectations(s.T())
}

func (s *PublisherTestSuite) TestPublishTopicError() {
	assertions := s.Assertions

	ctx := context.Background()

	data := FakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	message, err := NewMessage(s.settings, "user-created", "2.0", nil, &data)
	s.Require().NoError(err)

	payload := []byte(`{"type": "user-created"}`)
	headers := map[string]string{}

	s.validator.On("Serialize", message).
		Return(payload, headers, nil)

	_, err = s.publisher.Publish(ctx, message)
	assertions.Error(err, "Message route is not defined for message")

	s.backend.AssertExpectations(s.T())
}

func (s *PublisherTestSuite) TestNew() {
	assert.NotNil(s.T(), s.publisher)
}

type PublisherTestSuite struct {
	suite.Suite
	publisher *Publisher
	backend   *FakeBackend
	validator *FakeValidator
	callback  *fakeCallback
	settings  *Settings
}

func (s *PublisherTestSuite) SetupTest() {
	settings := &Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		QueueName:    "dev-myapp",
		MessageRouting: map[MessageRouteKey]string{
			{
				MessageType:         "user-created",
				MessageMajorVersion: 1,
			}: "dev-user-created-v1",
		},
	}
	backend := &FakeBackend{}
	validator := &FakeValidator{}

	s.publisher = NewPublisher(settings, backend, validator).(*Publisher)
	s.backend = backend
	s.validator = validator
	s.settings = settings
}

func TestPublisherTestSuite(t *testing.T) {
	suite.Run(t, &PublisherTestSuite{})
}
