/*
 * Author: Michael Ngo
 */

package hedwig

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Masterminds/semver"

	"github.com/stretchr/testify/suite"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type fakeLog struct {
	level   string
	err     error
	message string
	fields  LoggingFields
}

type fakeLogger struct {
	lock sync.Mutex
	logs []fakeLog
}

func (f *fakeLogger) Error(err error, message string, fields LoggingFields) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"error", err, message, fields})
}

func (f *fakeLogger) Warn(err error, message string, fields LoggingFields) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"warn", err, message, fields})
}

func (f *fakeLogger) Info(message string, fields LoggingFields) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"info", nil, message, fields})
}

func (f *fakeLogger) Debug(message string, fields LoggingFields) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"debug", nil, message, fields})
}

type fakeCallback struct {
	mock.Mock
}

func (fc *fakeCallback) Callback(ctx context.Context, m *Message) error {
	args := fc.Called(ctx, m)
	return args.Error(0)
}

type FakeBackend struct {
	mock.Mock
}

func (b *FakeBackend) Receive(ctx context.Context, numMessages uint32, visibilityTimeoutS uint32, callback ConsumerCallback) error {
	args := b.Called(ctx, numMessages, visibilityTimeoutS, callback)
	return args.Error(0)
}

func (b *FakeBackend) NackMessage(ctx context.Context, providerMetadata interface{}) error {
	args := b.Called(ctx, providerMetadata)
	return args.Error(0)
}

func (b *FakeBackend) AckMessage(ctx context.Context, providerMetadata interface{}) error {
	args := b.Called(ctx, providerMetadata)
	return args.Error(0)
}

func (b *FakeBackend) Publish(ctx context.Context, message *Message, payload []byte, attributes map[string]string, topic string) (string, error) {
	args := b.Called(ctx, message, payload, attributes, topic)
	return args.String(0), args.Error(1)
}

func (s *ConsumerTestSuite) TestProcessMessage() {
	ctx := context.Background()
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	message := Message{Type: "user-created", DataSchemaVersion: semver.MustParse("1.0")}
	s.validator.On("Deserialize", payload, attributes, providerMetadata).
		Return(&message, nil)
	s.callback.On("Callback", ctx, &message).
		Return(nil)
	s.backend.On("AckMessage", ctx, providerMetadata).
		Return(nil)
	s.consumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.validator.AssertExpectations(s.T())
	s.callback.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestProcessMessageDeserializeFailure() {
	ctx := context.Background()
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	s.validator.On("Deserialize", payload, attributes, providerMetadata).
		Return((*Message)(nil), errors.New("invalid message"))
	s.consumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.Equal(len(s.logger.logs), 1)
	s.Equal(s.logger.logs[0].message, "invalid message, unable to unmarshal")
	s.Error(s.logger.logs[0].err, "invalid message")
	s.validator.AssertExpectations(s.T())
	s.callback.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestProcessMessageCallbackFailure() {
	ctx := context.Background()
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	message := Message{Type: "user-created", DataSchemaVersion: semver.MustParse("1.0")}
	s.validator.On("Deserialize", payload, attributes, providerMetadata).
		Return(&message, nil)
	s.callback.On("Callback", ctx, &message).
		Return(errors.New("failed to process"))
	s.backend.On("NackMessage", ctx, providerMetadata).
		Return(nil)
	s.consumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.Equal(len(s.logger.logs), 1)
	s.Equal(s.logger.logs[0].message, "Retrying due to unknown exception")
	s.Error(s.logger.logs[0].err, "failed to process")
	s.validator.AssertExpectations(s.T())
	s.callback.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestProcessNackFailure() {
	ctx := context.Background()
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	message := Message{Type: "user-created", DataSchemaVersion: semver.MustParse("1.0")}
	s.validator.On("Deserialize", payload, attributes, providerMetadata).
		Return(&message, nil)
	s.callback.On("Callback", ctx, &message).
		Return(errors.New("failed to process"))
	s.backend.On("NackMessage", ctx, providerMetadata).
		Return(errors.New("failed to nack"))
	s.consumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.Equal(len(s.logger.logs), 2)
	s.Equal(s.logger.logs[0].message, "Retrying due to unknown exception")
	s.Error(s.logger.logs[0].err, "failed to process")
	s.Equal(s.logger.logs[1].message, "Failed to nack message")
	s.Error(s.logger.logs[1].err, "failed to nack")
	s.validator.AssertExpectations(s.T())
	s.callback.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestProcessMessageAckFailure() {
	ctx := context.Background()
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	message := Message{Type: "user-created", DataSchemaVersion: semver.MustParse("1.0")}
	s.validator.On("Deserialize", payload, attributes, providerMetadata).
		Return(&message, nil)
	s.callback.On("Callback", ctx, &message).
		Return(nil)
	s.backend.On("AckMessage", ctx, providerMetadata).
		Return(errors.New("failed to ack"))
	s.consumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.Equal(len(s.logger.logs), 1)
	s.Equal(s.logger.logs[0].message, "Failed to ack message")
	s.Error(s.logger.logs[0].err, "failed to ack")
	s.validator.AssertExpectations(s.T())
	s.callback.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestListenForMessages() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeoutS := uint32(20)
	s.backend.On("Receive", ctx, numMessages, visibilityTimeoutS, mock.AnythingOfType("ConsumerCallback")).
		Return(context.Canceled).
		After(500 * time.Millisecond)
	err := s.consumer.ListenForMessages(ctx, ListenRequest{numMessages, visibilityTimeoutS})
	assert.EqualError(s.T(), err, "context canceled")
}

func (s *ConsumerTestSuite) TestNew() {
	assert.NotNil(s.T(), s.consumer)
}

type ConsumerTestSuite struct {
	suite.Suite
	consumer  *queueConsumer
	backend   *FakeBackend
	validator *FakeValidator
	callback  *fakeCallback
	logger    *fakeLogger
}

func (s *ConsumerTestSuite) SetupTest() {
	callback := &fakeCallback{}
	logger := &fakeLogger{}

	settings := &Settings{
		AWSRegion:        "us-east-1",
		AWSAccountID:     "1234567890",
		QueueName:        "dev-myapp",
		CallbackRegistry: &CallbackRegistry{CallbackKey{"user-created", 1}: callback.Callback},
		GetLogger:        func(_ context.Context) Logger { return logger },
	}
	backend := &FakeBackend{}
	validator := &FakeValidator{}

	s.consumer = NewQueueConsumer(settings, backend, validator).(*queueConsumer)
	s.backend = backend
	s.callback = callback
	s.validator = validator
	s.logger = logger
}

func TestConsumerTestSuite(t *testing.T) {
	suite.Run(t, &ConsumerTestSuite{})
}
