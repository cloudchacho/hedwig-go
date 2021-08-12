package gcp_test

import (
	"bytes"
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cloudchacho/hedwig-go/internal/testutils"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"

	"github.com/cloudchacho/hedwig-go"
	"github.com/cloudchacho/hedwig-go/gcp"
)

type fakeHedwigDataField struct {
	VehicleID string `json:"vehicle_id"`
}

type fakeValidator struct {
	mock.Mock
}

type fakeConsumerCallback struct {
	mock.Mock
}

func (fc *fakeConsumerCallback) Callback(ctx context.Context, payload []byte, attributes map[string]string, providerMetadata interface{}) {
	fc.Called(ctx, payload, attributes, providerMetadata)
}

func (f *fakeValidator) Serialize(message *hedwig.Message) ([]byte, map[string]string, error) {
	args := f.Called(message)
	return args.Get(0).([]byte), args.Get(1).(map[string]string), args.Error(2)
}

func (f *fakeValidator) Deserialize(messagePayload []byte, attributes map[string]string, providerMetadata interface{}) (*hedwig.Message, error) {
	args := f.Called(messagePayload, attributes, providerMetadata)
	return args.Get(0).(*hedwig.Message), args.Error(1)
}

func (s *BackendTestSuite) publish(payload []byte, attributes map[string]string, topic string) error {
	ctx := context.Background()
	_, err := s.client.Topic(topic).Publish(ctx, &pubsub.Message{
		Data:       payload,
		Attributes: attributes,
	}).Get(ctx)
	return err
}

func (s *BackendTestSuite) TestReceive() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10

	payload := []byte(`{"vehicle_id": "C_123"}`)
	attributes := map[string]string{
		"foo": "bar",
	}
	err := s.publish(payload, attributes, "hedwig-dev-user-created-v1")
	s.Require().NoError(err)

	payload2 := []byte("\xbd\xb2\x3d\xbc\x20\xe2\x8c\x98")
	attributes2 := map[string]string{
		"foo": "bar",
	}
	err = s.publish(payload2, attributes2, "hedwig-dev-user-created-v1")
	s.Require().NoError(err)

	s.fakeConsumerCallback.On("Callback", mock.AnythingOfType("*context.cancelCtx"), payload, attributes, mock.AnythingOfType("gcp.Metadata")).
		// message must be acked or Receive never returns
		Run(func(args mock.Arguments) {
			err := s.backend.AckMessage(ctx, args.Get(3))
			s.Require().NoError(err)
		}).
		Return().
		Once()
	s.fakeConsumerCallback.On("Callback", mock.AnythingOfType("*context.cancelCtx"), payload2, attributes2, mock.AnythingOfType("gcp.Metadata")).
		// message must be acked or Receive never returns
		Run(func(args mock.Arguments) {
			err := s.backend.AckMessage(ctx, args.Get(3))
			s.Require().NoError(err)
		}).
		Return().
		Once().
		// force method to return after just one loop
		After(time.Millisecond * 110)

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*200)
	defer cancel()
	testutils.RunAndWait(func() {
		err := s.backend.Receive(ctx, numMessages, visibilityTimeout, s.fakeConsumerCallback.Callback)
		s.True(err.Error() == "draining" || err == context.DeadlineExceeded)
	})

	s.fakeConsumerCallback.AssertExpectations(s.T())

	providerMetadata := s.fakeConsumerCallback.Mock.Calls[0].Arguments.Get(3).(gcp.Metadata)
	s.Equal(1, providerMetadata.DeliveryAttempt)
}

func (s *BackendTestSuite) TestReceiveCrossProject() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10

	s.settings.SubscriptionsCrossProject = []hedwig.SubscriptionProject{{"dev-user-created-v1", "other-project"}}
	s.settings.Subscriptions = []string{}

	payload := []byte(`{"vehicle_id": "C_123"}`)
	attributes := map[string]string{
		"foo": "bar",
	}
	err := s.publish(payload, attributes, "hedwig-dev-user-created-v1")
	s.Require().NoError(err)

	s.fakeConsumerCallback.On("Callback", mock.AnythingOfType("*context.cancelCtx"), payload, attributes, mock.AnythingOfType("gcp.Metadata")).
		// message must be acked or Receive never returns
		Run(func(args mock.Arguments) {
			err := s.backend.AckMessage(ctx, args.Get(3))
			s.Require().NoError(err)
		}).
		Return().
		Once()

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*200)
	defer cancel()
	testutils.RunAndWait(func() {
		err := s.backend.Receive(ctx, numMessages, visibilityTimeout, s.fakeConsumerCallback.Callback)
		s.True(err.Error() == "draining" || err == context.DeadlineExceeded)
	})

	s.fakeConsumerCallback.AssertExpectations(s.T())

	providerMetadata := s.fakeConsumerCallback.Mock.Calls[0].Arguments.Get(3).(gcp.Metadata)
	s.Equal(1, providerMetadata.DeliveryAttempt)
}

func (s *BackendTestSuite) TestReceiveNoMessages() {
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*200)
	defer cancel()
	testutils.RunAndWait(func() {
		err := s.backend.Receive(ctx, numMessages, visibilityTimeout, s.fakeConsumerCallback.Callback)
		s.True(err.Error() == "draining" || err == context.DeadlineExceeded)
	})

	s.fakeConsumerCallback.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestReceiveError() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10

	s.settings.QueueName = "does-not-exist"
	s.settings.Subscriptions = nil

	testutils.RunAndWait(func() {
		err := s.backend.Receive(ctx, numMessages, visibilityTimeout, s.fakeConsumerCallback.Callback)
		s.EqualError(err, "rpc error: code = NotFound desc = Subscription does not exist (resource=hedwig-does-not-exist)")
	})

	s.fakeConsumerCallback.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestRequeueDLQ() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10

	payload := []byte(`{"vehicle_id": "C_123"}`)
	attributes := map[string]string{
		"foo": "bar",
	}
	err := s.publish(payload, attributes, "hedwig-dev-myapp-dlq")
	s.Require().NoError(err)

	payload2 := []byte("\xbd\xb2\x3d\xbc\x20\xe2\x8c\x98")
	attributes2 := map[string]string{
		"foo": "bar",
	}
	err = s.publish(payload2, attributes2, "hedwig-dev-myapp-dlq")
	s.Require().NoError(err)

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*200)
	defer cancel()
	testutils.RunAndWait(func() {
		err := s.backend.RequeueDLQ(ctx, numMessages, visibilityTimeout)
		s.True(err.Error() == "draining" || err == context.DeadlineExceeded)
	})

	received := [2]int32{}
	ctx, cancel = context.WithTimeout(context.Background(), time.Millisecond*200)
	defer cancel()
	err = s.client.Subscription("hedwig-dev-myapp").Receive(ctx, func(_ context.Context, message *pubsub.Message) {
		if bytes.Equal(message.Data, payload) {
			atomic.AddInt32(&received[0], 1)
			s.Equal(message.Attributes, attributes)
		} else {
			s.Equal(message.Data, payload2)
			s.Equal(message.Attributes, attributes2)
			atomic.AddInt32(&received[1], 1)
		}
		if atomic.LoadInt32(&received[0]) >= 1 && atomic.LoadInt32(&received[0]) >= 1 {
			cancel()
		}
		message.Ack()
	})
	s.Require().NoError(err)
}

func (s *BackendTestSuite) TestRequeueDLQNoMessages() {
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	testutils.RunAndWait(func() {
		err := s.backend.RequeueDLQ(ctx, numMessages, visibilityTimeout)
		s.NoError(err)
	})
}

func (s *BackendTestSuite) TestRequeueDLQReceiveError() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10

	s.settings.QueueName = "does-not-exist"
	s.settings.Subscriptions = nil

	testutils.RunAndWait(func() {
		err := s.backend.RequeueDLQ(ctx, numMessages, visibilityTimeout)
		s.EqualError(err, "rpc error: code = NotFound desc = Subscription does not exist (resource=hedwig-does-not-exist-dlq)")
	})

	s.fakeConsumerCallback.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestRequeueDLQPublishError() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10

	payload := []byte(`{"vehicle_id": "C_123"}`)
	attributes := map[string]string{
		"foo": "bar",
	}
	err := s.publish(payload, attributes, "hedwig-dev-myapp-dlq")
	s.Require().NoError(err)

	topic := s.client.Topic("hedwig-dev-myapp")
	err = topic.Delete(ctx)
	s.Require().NoError(err)

	testutils.RunAndWait(func() {
		err := s.backend.RequeueDLQ(ctx, numMessages, visibilityTimeout)
		s.EqualError(err, "rpc error: code = NotFound desc = Topic not found")
	})
}

func (s *BackendTestSuite) TestPublish() {
	ctx, cancel := context.WithCancel(context.Background())

	msgTopic := "dev-user-created-v1"

	messageID, err := s.backend.Publish(ctx, s.message, s.payload, s.attributes, msgTopic)
	s.NoError(err)
	s.NotEmpty(messageID)

	err = s.client.Subscription("hedwig-dev-myapp-dev-user-created-v1").Receive(ctx, func(_ context.Context, message *pubsub.Message) {
		cancel()
		s.Equal(message.Data, s.payload)
		s.Equal(message.Attributes, s.attributes)
		message.Ack()
	})
	s.Require().NoError(err)
}

func (s *BackendTestSuite) TestPublishFailure() {
	ctx := context.Background()

	_, err := s.backend.Publish(ctx, s.message, s.payload, s.attributes, "does-not-exist")
	s.EqualError(err, "Failed to publish message to Pub/Sub: rpc error: code = NotFound desc = Topic not found")
}

func (s *BackendTestSuite) TestAck() {
	ctx := context.Background()

	msgTopic := "dev-user-created-v1"

	messageID, err := s.backend.Publish(ctx, s.message, s.payload, s.attributes, msgTopic)
	s.NoError(err)
	s.NotEmpty(messageID)

	ctx2, cancel2 := context.WithCancel(ctx)
	err = s.client.Subscription("hedwig-dev-myapp-dev-user-created-v1").Receive(ctx2, func(_ context.Context, message *pubsub.Message) {
		defer cancel2()
		s.Equal(message.Data, s.payload)
		s.Equal(message.Attributes, s.attributes)
		message.Ack()
	})
	s.NoError(err)

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*200)
	defer cancel()
	testutils.RunAndWait(func() {
		err := s.client.Subscription("hedwig-dev-myapp-dev-user-created-v1").Receive(ctx, func(_ context.Context, message *pubsub.Message) {
			s.Fail("shouldn't have received any message")
		})
		s.Require().NoError(err)
	})
}

func (s *BackendTestSuite) TestNack() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10

	msgTopic := "dev-user-created-v1"

	messageID, err := s.backend.Publish(ctx, s.message, s.payload, s.attributes, msgTopic)
	s.NoError(err)
	s.NotEmpty(messageID)

	s.fakeConsumerCallback.On("Callback", mock.AnythingOfType("*context.cancelCtx"), s.payload, s.attributes, mock.AnythingOfType("gcp.Metadata")).
		Run(func(args mock.Arguments) {
			err := s.backend.NackMessage(ctx, args.Get(3))
			s.Require().NoError(err)
		}).
		Return()

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*200)
	defer cancel()
	testutils.RunAndWait(func() {
		err := s.backend.Receive(ctx, numMessages, visibilityTimeout, s.fakeConsumerCallback.Callback)
		s.True(err.Error() == "draining" || err == context.DeadlineExceeded)
	})

	s.fakeConsumerCallback.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestNew() {
	assert.NotNil(s.T(), s.backend)
}

type BackendTestSuite struct {
	suite.Suite
	backend              hedwig.IBackend
	client               *pubsub.Client
	settings             *hedwig.Settings
	message              *hedwig.Message
	payload              []byte
	attributes           map[string]string
	validator            *fakeValidator
	fakeConsumerCallback *fakeConsumerCallback
}

func (s *BackendTestSuite) SetupSuite() {
	s.TearDownSuite()
	ctx := context.Background()
	if s.client == nil {
		client, err := pubsub.NewClient(ctx, "emulator-project")
		s.Require().NoError(err)
		s.client = client
	}
	dlqTopic, err := s.client.CreateTopic(ctx, "hedwig-dev-myapp-dlq")
	s.Require().NoError(err)
	_, err = s.client.CreateSubscription(ctx, "hedwig-dev-myapp-dlq", pubsub.SubscriptionConfig{
		Topic:       dlqTopic,
		AckDeadline: time.Second * 20,
	})
	s.Require().NoError(err)
	topic, err := s.client.CreateTopic(ctx, "hedwig-dev-user-created-v1")
	s.Require().NoError(err)
	_, err = s.client.CreateSubscription(ctx, "hedwig-dev-myapp-dev-user-created-v1", pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: time.Second * 20,
		DeadLetterPolicy: &pubsub.DeadLetterPolicy{
			DeadLetterTopic:     dlqTopic.String(),
			MaxDeliveryAttempts: 5,
		},
	})
	s.Require().NoError(err)
	_, err = s.client.CreateSubscription(ctx, "hedwig-dev-myapp-other-project-dev-user-created-v1", pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: time.Second * 20,
		DeadLetterPolicy: &pubsub.DeadLetterPolicy{
			DeadLetterTopic:     dlqTopic.String(),
			MaxDeliveryAttempts: 5,
		},
	})
	s.Require().NoError(err)
	topic, err = s.client.CreateTopic(ctx, "hedwig-dev-myapp")
	s.Require().NoError(err)
	_, err = s.client.CreateSubscription(ctx, "hedwig-dev-myapp", pubsub.SubscriptionConfig{
		Topic:       topic,
		AckDeadline: time.Second * 20,
		DeadLetterPolicy: &pubsub.DeadLetterPolicy{
			DeadLetterTopic:     dlqTopic.String(),
			MaxDeliveryAttempts: 5,
		},
	})
	s.Require().NoError(err)
}

func (s *BackendTestSuite) TearDownSuite() {
	ctx := context.Background()
	if s.client == nil {
		client, err := pubsub.NewClient(
			ctx,
			"emulator-project",
			option.WithoutAuthentication(),
			option.WithGRPCDialOption(grpc.WithInsecure()),
		)
		s.Require().NoError(err)
		s.client = client
	}
	defer func() {
		s.Require().NoError(s.client.Close())
		s.client = nil
	}()
	subscriptions := s.client.Subscriptions(ctx)
	for {
		if subscription, err := subscriptions.Next(); err == iterator.Done {
			break
		} else if err != nil {
			panic(fmt.Sprintf("failed to delete subscriptions with error: %v", err))
		} else {
			err = subscription.Delete(ctx)
			s.Require().NoError(err)
		}
	}
	topics := s.client.Topics(ctx)
	for {
		if topic, err := topics.Next(); err == iterator.Done {
			break
		} else if err != nil {
			panic(fmt.Sprintf("failed to delete topics with error: %v", err))
		} else {
			err = topic.Delete(ctx)
			s.Require().NoError(err)
		}
	}
}

func (s *BackendTestSuite) SetupTest() {
	settings := &hedwig.Settings{
		GoogleCloudProject: "emulator-project",
		QueueName:          "dev-myapp",
		MessageRouting: map[hedwig.MessageTypeMajorVersion]string{
			{
				MessageType:  "user-created",
				MajorVersion: 1,
			}: "dev-user-created-v1",
		},
		Subscriptions:   []string{"dev-user-created-v1"},
		ShutdownTimeout: time.Second * 10,
	}
	fakeMessageCallback := &fakeConsumerCallback{}
	message, err := hedwig.NewMessage(settings, "user-created", "1.0", map[string]string{"foo": "bar"}, &fakeHedwigDataField{})
	require.NoError(s.T(), err)

	validator := &fakeValidator{}

	payload := []byte(`{"vehicle_id": "C_123"}`)
	attributes := map[string]string{"foo": "bar"}

	s.backend = gcp.NewBackend(settings)
	s.settings = settings
	s.message = message
	s.validator = validator
	s.payload = payload
	s.attributes = attributes
	s.fakeConsumerCallback = fakeMessageCallback
}

func (s *BackendTestSuite) TearDownTest() {
	ctx := context.Background()
	subscriptions := s.client.Subscriptions(ctx)
	for {
		if subscription, err := subscriptions.Next(); err == iterator.Done {
			break
		} else if err != nil {
			panic(fmt.Sprintf("failed to delete subscriptions with error: %v", err))
		} else {
			err = subscription.SeekToTime(ctx, time.Now())
			s.Require().NoError(err)
		}
	}
}

func TestBackendTestSuite(t *testing.T) {
	suite.Run(t, &BackendTestSuite{})
}
