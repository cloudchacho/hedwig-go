package hedwig

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Masterminds/semver"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
)

type fakeLog struct {
	level   string
	err     error
	message string
	fields  []interface{}
}

type fakeLogger struct {
	lock sync.Mutex
	logs []fakeLog
}

func (f *fakeLogger) Error(_ context.Context, err error, message string, keyvals ...interface{}) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"error", err, message, keyvals})
}

func (f *fakeLogger) Debug(_ context.Context, message string, keyvals ...interface{}) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"debug", nil, message, keyvals})
}

type fakeCallback struct {
	mock.Mock
}

func (fc *fakeCallback) Callback(ctx context.Context, m *Message) error {
	args := fc.Called(ctx, m)
	return args.Error(0)
}

type fakeBackend struct {
	mock.Mock
}

func (b *fakeBackend) Receive(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration, messageCh chan<- ReceivedMessage) error {
	args := b.Called(ctx, numMessages, visibilityTimeout, messageCh)
	return args.Error(0)
}

func (b *fakeBackend) NackMessage(ctx context.Context, providerMetadata interface{}) error {
	args := b.Called(ctx, providerMetadata)
	return args.Error(0)
}

func (b *fakeBackend) AckMessage(ctx context.Context, providerMetadata interface{}) error {
	args := b.Called(ctx, providerMetadata)
	return args.Error(0)
}

func (b *fakeBackend) Publish(ctx context.Context, message *Message, payload []byte, attributes map[string]string, topic string) (string, error) {
	args := b.Called(ctx, message, payload, attributes, topic)
	return args.String(0), args.Error(1)
}

func (b *fakeBackend) RequeueDLQ(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration) error {
	args := b.Called(ctx, numMessages, visibilityTimeout)
	return args.Error(0)
}

type fakeInstrumenter struct {
	mock.Mock
}

func (f *fakeInstrumenter) OnMessageDeserialized(ctx context.Context, message *Message) {
	f.Called(ctx, message)
}

func (f *fakeInstrumenter) OnPublish(ctx context.Context, message *Message, attributes map[string]string) (context.Context, map[string]string, func()) {
	args := f.Called(ctx, message, attributes)
	return args.Get(0).(context.Context), args.Get(1).(map[string]string), args.Get(2).(func())
}

func (f *fakeInstrumenter) OnReceive(ctx context.Context, attributes map[string]string) (context.Context, func()) {
	args := f.Called(ctx, attributes)
	return args.Get(0).(context.Context), args.Get(1).(func())
}

func (s *ConsumerTestSuite) TestProcessMessage() {
	ctx := context.Background()
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	message := Message{Type: "user-created", DataSchemaVersion: semver.MustParse("1.0")}
	s.deserializer.On("deserialize", payload, attributes, providerMetadata).
		Return(&message, nil)
	s.callback.On("Callback", ctx, &message).
		Return(nil)
	s.backend.On("AckMessage", ctx, providerMetadata).
		Return(nil)
	s.consumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.backend.AssertExpectations(s.T())
	s.deserializer.AssertExpectations(s.T())
	s.callback.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestProcessMessageDeserializeFailure() {
	ctx := context.Background()
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	s.deserializer.On("deserialize", payload, attributes, providerMetadata).
		Return((*Message)(nil), errors.New("invalid message"))
	s.backend.On("NackMessage", ctx, providerMetadata).
		Return(nil)
	s.consumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.Equal(len(s.logger.logs), 1)
	s.Equal(s.logger.logs[0].message, "invalid message, unable to unmarshal")
	s.EqualError(s.logger.logs[0].err, "invalid message")
	s.backend.AssertExpectations(s.T())
	s.deserializer.AssertExpectations(s.T())
	s.callback.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestProcessMessageCallbackFailure() {
	ctx := context.Background()
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	message := Message{Type: "user-created", DataSchemaVersion: semver.MustParse("1.0")}
	s.deserializer.On("deserialize", payload, attributes, providerMetadata).
		Return(&message, nil)
	s.callback.On("Callback", ctx, &message).
		Return(errors.New("failed to process"))
	s.backend.On("NackMessage", ctx, providerMetadata).
		Return(nil)
	s.consumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.Equal(len(s.logger.logs), 1)
	s.Equal(s.logger.logs[0].message, "Retrying due to unknown exception")
	s.EqualError(s.logger.logs[0].err, "failed to process")
	s.backend.AssertExpectations(s.T())
	s.deserializer.AssertExpectations(s.T())
	s.callback.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestProcessMessageCallbackPanic() {
	ctx := context.Background()
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	message := Message{Type: "user-created", DataSchemaVersion: semver.MustParse("1.0")}
	s.deserializer.On("deserialize", payload, attributes, providerMetadata).
		Return(&message, nil)
	s.callback.On("Callback", ctx, &message).
		Panic("failed to process")
	s.backend.On("NackMessage", ctx, providerMetadata).
		Return(nil)
	s.consumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.Equal(len(s.logger.logs), 1)
	s.Equal(s.logger.logs[0].message, "Retrying due to unknown exception")
	s.EqualError(s.logger.logs[0].err, "panic: failed to process")
	s.backend.AssertExpectations(s.T())
	s.deserializer.AssertExpectations(s.T())
	s.callback.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestProcessMessageCallbackErrRetry() {
	ctx := context.Background()
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	message := Message{Type: "user-created", DataSchemaVersion: semver.MustParse("1.0")}
	s.deserializer.On("deserialize", payload, attributes, providerMetadata).
		Return(&message, nil)
	s.callback.On("Callback", ctx, &message).
		Return(ErrRetry)
	s.backend.On("NackMessage", ctx, providerMetadata).
		Return(nil)
	s.consumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.Equal(len(s.logger.logs), 1)
	s.Equal(s.logger.logs[0].message, "Retrying due to exception")
	s.NoError(s.logger.logs[0].err)
	s.backend.AssertExpectations(s.T())
	s.deserializer.AssertExpectations(s.T())
	s.callback.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestProcessMessageCallbackNotFound() {
	ctx := context.Background()
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	message := Message{Type: "user-created", DataSchemaVersion: semver.MustParse("1.0")}
	s.deserializer.On("deserialize", payload, attributes, providerMetadata).
		Return(&message, nil)
	delete(s.consumer.registry, MessageTypeMajorVersion{"user-created", 1})
	s.backend.On("NackMessage", ctx, providerMetadata).
		Return(nil)
	s.consumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.Equal(len(s.logger.logs), 1)
	s.Equal(s.logger.logs[0].message, "no callback defined for message")
	s.EqualError(s.logger.logs[0].err, "no callback defined for message")
	s.backend.AssertExpectations(s.T())
	s.deserializer.AssertExpectations(s.T())
	s.callback.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestProcessNackFailure() {
	ctx := context.Background()
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	message := Message{Type: "user-created", DataSchemaVersion: semver.MustParse("1.0")}
	s.deserializer.On("deserialize", payload, attributes, providerMetadata).
		Return(&message, nil)
	s.callback.On("Callback", ctx, &message).
		Return(errors.New("failed to process"))
	s.backend.On("NackMessage", ctx, providerMetadata).
		Return(errors.New("failed to nack"))
	s.consumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.Equal(len(s.logger.logs), 2)
	s.Equal(s.logger.logs[0].message, "Retrying due to unknown exception")
	s.EqualError(s.logger.logs[0].err, "failed to process")
	s.Equal(s.logger.logs[1].message, "Failed to nack message")
	s.EqualError(s.logger.logs[1].err, "failed to nack")
	s.backend.AssertExpectations(s.T())
	s.deserializer.AssertExpectations(s.T())
	s.callback.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestProcessMessageAckFailure() {
	ctx := context.Background()
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	message := Message{Type: "user-created", DataSchemaVersion: semver.MustParse("1.0")}
	s.deserializer.On("deserialize", payload, attributes, providerMetadata).
		Return(&message, nil)
	s.callback.On("Callback", ctx, &message).
		Return(nil)
	s.backend.On("AckMessage", ctx, providerMetadata).
		Return(errors.New("failed to ack"))
	s.backend.On("NackMessage", ctx, providerMetadata).
		Return(nil)
	s.consumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.Equal(len(s.logger.logs), 1)
	s.Equal(s.logger.logs[0].message, "Failed to ack message")
	s.EqualError(s.logger.logs[0].err, "failed to ack")
	s.backend.AssertExpectations(s.T())
	s.deserializer.AssertExpectations(s.T())
	s.callback.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestProcessMessageFollowsParentTrace() {
	ctx := context.Background()
	instrumentedCtx := context.WithValue(ctx, contextKey("instrumented"), true)
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123", "traceparent": "00-aa2ada259e917551e16da4a0ad33db24-662fd261d30ec74c-01"}
	providerMetadata := struct{}{}
	instrumenter := &fakeInstrumenter{}
	instrumentedConsumer := s.consumer.WithInstrumenter(instrumenter)
	message := Message{Type: "user-created", DataSchemaVersion: semver.MustParse("1.0")}
	s.deserializer.On("deserialize", payload, attributes, providerMetadata).
		Return(&message, nil)
	s.callback.On("Callback", instrumentedCtx, &message).
		Return(nil)
	s.backend.On("AckMessage", instrumentedCtx, providerMetadata).
		Return(nil)
	called := false
	instrumenter.On("OnReceive", ctx, attributes).
		Return(instrumentedCtx, func() { called = true })
	instrumenter.On("OnMessageDeserialized", instrumentedCtx, &message)
	instrumentedConsumer.processMessage(ctx, payload, attributes, providerMetadata)
	s.backend.AssertExpectations(s.T())
	s.deserializer.AssertExpectations(s.T())
	s.callback.AssertExpectations(s.T())
	instrumenter.AssertExpectations(s.T())
	s.True(called)
}

func (s *ConsumerTestSuite) TestListenForMessages() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 20
	payload := []byte(`foobar`)
	attributes := map[string]string{"request_id": "123"}
	providerMetadata := struct{}{}
	payload2 := []byte(`foobar2`)
	attributes2 := map[string]string{"request_id": "456"}
	providerMetadata2 := struct{}{}
	message := Message{Type: "user-created", DataSchemaVersion: semver.MustParse("1.0")}
	s.deserializer.On("deserialize", payload, attributes, providerMetadata).
		Return(&message, nil)
	s.deserializer.On("deserialize", payload2, attributes2, providerMetadata2).
		Return(&message, nil)
	s.callback.On("Callback", ctx, &message).
		Return(nil).
		After(time.Millisecond * 50)
	s.callback.On("Callback", ctx, &message).
		Return(nil)
	s.backend.On("AckMessage", ctx, providerMetadata).
		Return(nil)
	s.backend.On("AckMessage", ctx, providerMetadata2).
		Return(nil)
	s.backend.On("Receive", ctx, numMessages, visibilityTimeout, mock.AnythingOfType("chan<- hedwig.ReceivedMessage")).
		Return(context.Canceled).
		Run(func(args mock.Arguments) {
			ch := args.Get(3).(chan<- ReceivedMessage)
			ch <- ReceivedMessage{
				Payload:          payload,
				Attributes:       attributes,
				ProviderMetadata: providerMetadata,
			}
			ch <- ReceivedMessage{
				Payload:          payload2,
				Attributes:       attributes2,
				ProviderMetadata: providerMetadata2,
			}
		}).
		After(500 * time.Millisecond)
	err := s.consumer.ListenForMessages(ctx, ListenRequest{numMessages, visibilityTimeout, 1})
	assert.EqualError(s.T(), err, "context canceled")
	s.backend.AssertExpectations(s.T())
	s.deserializer.AssertExpectations(s.T())
	s.callback.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestUseTransportMessageAttributes() {
	s.deserializer.On("withUseTransportMessageAttributes", false).
		Return(s.deserializer, nil)
	s.consumer.WithUseTransportMessageAttributes(false)
	s.deserializer.AssertExpectations(s.T())
}

func (s *ConsumerTestSuite) TestNew() {
	assert.NotNil(s.T(), s.consumer)
}

type fakeDeserializer struct {
	mock.Mock
}

func (f *fakeDeserializer) deserialize(messagePayload []byte, attributes map[string]string, providerMetadata interface{}) (*Message, error) {
	args := f.Called(messagePayload, attributes, providerMetadata)
	return args.Get(0).(*Message), args.Error(1)
}

func (f *fakeDeserializer) withUseTransportMessageAttributes(useTransportMessageAttributes bool) {
	f.Called(useTransportMessageAttributes)
}

type ConsumerTestSuite struct {
	suite.Suite
	consumer     *QueueConsumer
	backend      *fakeBackend
	callback     *fakeCallback
	logger       *fakeLogger
	deserializer *fakeDeserializer
}

func (s *ConsumerTestSuite) SetupTest() {
	callback := &fakeCallback{}
	logger := &fakeLogger{}

	registry := CallbackRegistry{MessageTypeMajorVersion{"user-created", 1}: callback.Callback}
	backend := &fakeBackend{}
	deserializer := &fakeDeserializer{}

	s.consumer = NewQueueConsumer(backend, nil, logger, registry)
	s.consumer.deserializer = deserializer
	s.backend = backend
	s.callback = callback
	s.deserializer = deserializer
	s.logger = logger
}

func TestConsumerTestSuite(t *testing.T) {
	suite.Run(t, &ConsumerTestSuite{})
}

func TestConsumer_DefaultLogger(t *testing.T) {
	backend := &fakeBackend{}

	consumer := NewQueueConsumer(backend, nil, nil, nil)
	assert.NotNil(t, consumer.logger)
}
