package hedwig

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/mock"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

func (s *PublisherTestSuite) TestPublish() {
	ctx := context.Background()

	data := fakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	message, err := NewMessage("user-created", "1.0", nil, &data, "myapp")
	s.Require().NoError(err)

	payload := []byte(`{"type": "user-created"}`)
	headers := map[string]string{}

	s.serializer.On("serialize", message, (*bool)(nil)).
		Return(payload, headers, nil)

	messageID := "123"

	s.backend.On("Publish", ctx, message, payload, headers, "dev-user-created-v1").
		Return(messageID, nil)

	receivedMessageID, err := s.publisher.Publish(ctx, message)
	s.Nil(err)
	s.Equal(messageID, receivedMessageID)

	s.backend.AssertExpectations(s.T())
}

func (s *PublisherTestSuite) TestPublishTopicError() {
	ctx := context.Background()

	data := fakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	message, err := NewMessage("user-created", "2.0", nil, &data, "myapp")
	s.Require().NoError(err)

	payload := []byte(`{"type": "user-created"}`)
	headers := map[string]string{}

	s.serializer.On("serialize", message, (*bool)(nil)).
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
	message, err := NewMessage("user-created", "2.0", nil, &data, "myapp")
	s.Require().NoError(err)

	s.serializer.On("serialize", message, (*bool)(nil)).
		Return([]byte(""), map[string]string{}, errors.New("failed to serialize"))

	_, err = s.publisher.Publish(ctx, message)
	s.EqualError(err, "failed to serialize")

	s.backend.AssertExpectations(s.T())
}

type contextKey string

func (s *PublisherTestSuite) TestPublishSendsTraceID() {
	ctx := context.Background()
	instrumentedCtx := context.WithValue(ctx, contextKey("instrumented"), true)

	data := fakeHedwigDataField{
		VehicleID: "C_1234567890123456",
	}
	message, err := NewMessage("user-created", "1.0", nil, &data, "myapp")
	s.Require().NoError(err)

	payload := []byte(`{"type": "user-created"}`)
	headers := map[string]string{}
	instrumentedHeaders := map[string]string{"traceparent": "00-aa2ada259e917551e16da4a0ad33db24-662fd261d30ec74c-01"}

	instrumenter := &fakeInstrumenter{}
	s.publisher.WithInstrumenter(instrumenter)

	called := false

	instrumenter.On("OnPublish", ctx, message, headers).
		Return(instrumentedCtx, instrumentedHeaders, func() { called = true })

	s.serializer.On("serialize", message, (*bool)(nil)).
		Return(payload, headers, nil)

	messageID := "123"

	s.backend.On("Publish", instrumentedCtx, message, payload, instrumentedHeaders, "dev-user-created-v1").
		Return(messageID, nil)

	receivedMessageID, err := s.publisher.Publish(ctx, message)
	s.Nil(err)
	s.Equal(messageID, receivedMessageID)

	s.backend.AssertExpectations(s.T())
	instrumenter.AssertExpectations(s.T())
	s.True(called)
}

func (s *PublisherTestSuite) TestUseTransportMessageAttributes() {
	s.serializer.On("withUseTransportMessageAttributes", false).
		Return(s.serializer, nil)
	s.publisher.WithUseTransportMessageAttributes(false)
	s.serializer.AssertExpectations(s.T())
}

func (s *PublisherTestSuite) TestNew() {
	assert.NotNil(s.T(), s.publisher)
}

type PublisherTestSuite struct {
	suite.Suite
	publisher  *Publisher
	backend    *fakeBackend
	serializer *fakeSerializer
}

type fakeSerializer struct {
	mock.Mock
}

func (f *fakeSerializer) serialize(message *Message, runWithTransportMessageAttributes *bool) ([]byte, map[string]string, error) {
	args := f.Called(message, runWithTransportMessageAttributes)
	return args.Get(0).([]byte), args.Get(1).(map[string]string), args.Error(2)
}

func (f *fakeSerializer) withUseTransportMessageAttributes(useTransportMessageAttributes bool) {
	f.Called(useTransportMessageAttributes)
}

func (s *PublisherTestSuite) SetupTest() {
	routing := map[MessageTypeMajorVersion]string{
		{
			MessageType:  "user-created",
			MajorVersion: 1,
		}: "dev-user-created-v1",
	}
	backend := &fakeBackend{}
	serializer := &fakeSerializer{}

	s.publisher = NewPublisher(backend, nil, routing)
	s.publisher.serializer = serializer
	s.backend = backend
	s.serializer = serializer
}

func TestPublisherTestSuite(t *testing.T) {
	suite.Run(t, &PublisherTestSuite{})
}
