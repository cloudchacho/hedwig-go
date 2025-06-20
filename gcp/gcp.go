package gcp

import (
	"context"
	"fmt"
	"math"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"

	"github.com/cloudchacho/hedwig-go"
)

type Backend struct {
	client   *pubsub.Client
	once     sync.Once
	settings Settings
	logger   hedwig.Logger
}

var _ = hedwig.ConsumerBackend(&Backend{})
var _ = hedwig.PublisherBackend(&Backend{})

const defaultVisibilityTimeoutS = time.Second * 20

// Metadata is additional metadata associated with a message
type Metadata struct {
	// Underlying pubsub message - ack id isn't exported so we have to store this object
	pubsubMessage *pubsub.Message

	// PublishTime is the time this message was originally published to Pub/Sub
	PublishTime time.Time

	// DeliveryAttempt is the counter received from Pub/Sub.
	//    The first delivery of a given message will have this value as 1. The value
	//    is calculated as best effort and is approximate.
	DeliveryAttempt int

	// The name of the subscription the message was received from
	SubscriptionName string
}

// Publish a message represented by the payload, with specified attributes to the specific topic
func (b *Backend) Publish(ctx context.Context, _ *hedwig.Message, payload []byte, attributes map[string]string, topic string) (string, error) {
	err := b.ensureClient(ctx)
	if err != nil {
		return "", err
	}

	clientTopic := b.client.Topic(fmt.Sprintf("hedwig-%s", topic))
	defer clientTopic.Stop()

	result := clientTopic.Publish(
		ctx,
		&pubsub.Message{
			Data:       payload,
			Attributes: attributes,
		},
	)
	messageID, err := result.Get(ctx)
	if err != nil {
		return "", errors.Wrap(err, "Failed to publish message to Pub/Sub")
	}
	return messageID, nil
}

// Receive messages from configured queue(s) and provide it through the callback. This should run indefinitely
// until the context is canceled. Provider metadata should include all info necessary to ack/nack a message.
func (b *Backend) Receive(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration, messageCh chan<- hedwig.ReceivedMessage) error {
	err := b.ensureClient(ctx)
	if err != nil {
		return err
	}

	defer b.client.Close()

	subscriptions := []string{}
	// all subscriptions live in an app's project, but cross-project subscriptions are named differently
	for _, subscription := range b.settings.SubscriptionsCrossProject {
		subscriptionName := fmt.Sprintf("hedwig-%s-%s-%s", b.settings.QueueName, subscription.ProjectID, subscription.Subscription)
		subscriptions = append(subscriptions, subscriptionName)
	}
	for _, subscription := range b.settings.Subscriptions {
		subscriptionName := fmt.Sprintf("hedwig-%s-%s", b.settings.QueueName, subscription)
		subscriptions = append(subscriptions, subscriptionName)
	}
	// main queue for DLQ re-queued messages
	subscriptionName := fmt.Sprintf("hedwig-%s", b.settings.QueueName)
	subscriptions = append(subscriptions, subscriptionName)

	hedwigRe := regexp.MustCompile(`^hedwig-`)
	group, gctx := errgroup.WithContext(ctx)

	for _, subscription := range subscriptions {
		pubsubSubscription := b.client.Subscription(subscription)
		subID := pubsubSubscription.ID()
		pubsubSubscription.ReceiveSettings.NumGoroutines = 1
		pubsubSubscription.ReceiveSettings.MaxOutstandingMessages = int(numMessages)
		if visibilityTimeout != 0 {
			pubsubSubscription.ReceiveSettings.MaxExtensionPeriod = visibilityTimeout
		} else {
			pubsubSubscription.ReceiveSettings.MaxExtensionPeriod = defaultVisibilityTimeoutS
		}
		group.Go(func() error {
			recvErr := pubsubSubscription.Receive(gctx, func(_ context.Context, message *pubsub.Message) {
				// deliveryAttempt is nil for subscriptions without a dlq
				deliveryAttemptDefault := -1
				if message.DeliveryAttempt == nil {
					message.DeliveryAttempt = &deliveryAttemptDefault
				}
				metadata := Metadata{
					pubsubMessage:    message,
					PublishTime:      message.PublishTime,
					DeliveryAttempt:  *message.DeliveryAttempt,
					SubscriptionName: hedwigRe.ReplaceAllString(subID, ""),
				}
				messageCh <- hedwig.ReceivedMessage{
					Payload:          message.Data,
					Attributes:       message.Attributes,
					ProviderMetadata: metadata,
				}
			})
			return recvErr
		})
	}

	err = group.Wait()
	if err != nil {
		return err
	}
	// context cancelation doesn't return error in the group
	return ctx.Err()
}

// RequeueDLQ re-queues everything in the Hedwig DLQ back into the Hedwig queue
func (b *Backend) RequeueDLQ(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration, numConcurrency uint32) error {
	err := b.ensureClient(ctx)
	if err != nil {
		return err
	}

	defer b.client.Close()

	clientTopic := b.client.Topic(fmt.Sprintf("hedwig-%s", b.settings.QueueName))
	defer clientTopic.Stop()

	clientTopic.PublishSettings.CountThreshold = int(numMessages)
	clientTopic.PublishSettings.FlowControlSettings.MaxOutstandingMessages = int(numMessages)
	clientTopic.PublishSettings.NumGoroutines = int(numConcurrency)
	if visibilityTimeout != 0 {
		clientTopic.PublishSettings.Timeout = visibilityTimeout
	} else {
		clientTopic.PublishSettings.Timeout = defaultVisibilityTimeoutS
	}

	// PublishSettings.BufferedByteLimit does not have an unlimited. Mimic what is
	// being set here in the library when PublishSettings.FlowControlSettings.MaxOutstandingBytes is set
	// ref: https://github.com/googleapis/google-cloud-go/blob/d8b933189d677e987f408fa45b50d134a418e2b0/pubsub/topic.go#L654
	clientTopic.PublishSettings.BufferedByteLimit = math.MaxInt64

	pubsubSubscription := b.client.Subscription(fmt.Sprintf("hedwig-%s-dlq", b.settings.QueueName))
	pubsubSubscription.ReceiveSettings.MaxOutstandingMessages = int(numMessages)
	pubsubSubscription.ReceiveSettings.MaxExtensionPeriod = clientTopic.PublishSettings.Timeout
	pubsubSubscription.ReceiveSettings.NumGoroutines = int(numConcurrency)

	// run a ticker that will fire after timeout and shutdown subscriber
	overallTimeout := time.Second * 30
	ticker := time.NewTicker(overallTimeout)
	defer ticker.Stop()

	wg := sync.WaitGroup{}
	defer wg.Wait()

	rctx, cancel := context.WithCancel(ctx)
	defer cancel()

	wg.Add(1)
	go func() {
		select {
		case <-ticker.C:
			cancel()
		case <-rctx.Done():
		}
		wg.Done()
	}()

	var numMessagesRequeued uint32

	progressTicker := time.NewTicker(time.Second * 1)
	defer progressTicker.Stop()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-progressTicker.C:
				b.logger.Debug(ctx, "Re-queue DLQ progress", "num_messages", atomic.LoadUint32(&numMessagesRequeued))
			case <-rctx.Done():
				return
			}
		}
	}()

	publishErrCh := make(chan error, 1)
	defer close(publishErrCh)
	err = pubsubSubscription.Receive(rctx, func(_ context.Context, message *pubsub.Message) {
		ticker.Reset(overallTimeout)
		result := clientTopic.Publish(rctx, message)
		_, err := result.Get(rctx)
		if err != nil {
			message.Nack()
			cancel()
			if err != context.Canceled {
				// try to send but since channel is buffered, this may not work, ignore since we anyway only want just
				// one error
				select {
				case publishErrCh <- err:
				default:
				}
			}
		} else {
			message.Ack()
			atomic.AddUint32(&numMessagesRequeued, 1)
		}
	})
	if err != nil {
		return err
	}
	// if publish failed, signal that
	select {
	case err = <-publishErrCh:
		return err
	default:
	}
	// context cancelation doesn't return error in Receive, don't return error from rctx since cancelation of rctx is
	// happy path
	return ctx.Err()
}

// NackMessage nacks a message on the queue
func (b *Backend) NackMessage(_ context.Context, providerMetadata interface{}) error {
	providerMetadata.(Metadata).pubsubMessage.Nack()
	return nil
}

// AckMessage acknowledges a message on the queue
func (b *Backend) AckMessage(_ context.Context, providerMetadata interface{}) error {
	providerMetadata.(Metadata).pubsubMessage.Ack()
	return nil
}

func (b *Backend) ensureClient(ctx context.Context) error {
	var err error
	b.once.Do(func() {
		googleCloudProject := b.settings.GoogleCloudProject
		if googleCloudProject == "" {
			var creds *google.Credentials
			creds, err = google.FindDefaultCredentials(ctx)
			if err != nil {
				err = errors.Wrap(
					err, "unable to discover google cloud project setting, either pass explicitly, or fix runtime environment")
			} else if creds.ProjectID == "" {
				err = errors.New(
					"unable to discover google cloud project setting, either pass explicitly, or fix runtime environment")
			}
			googleCloudProject = creds.ProjectID
		}
		b.client, err = pubsub.NewClient(context.Background(), googleCloudProject, b.settings.PubsubClientOptions...)
	})
	return err
}

// SubscriptionProject represents a tuple of subscription name and project for cross-project Google subscriptions
type SubscriptionProject struct {
	// Subscription name
	Subscription string

	// ProjectID
	ProjectID string
}

// Settings for Hedwig
type Settings struct {
	// Hedwig queue name. Exclude the `HEDWIG-` prefix
	QueueName string

	// GoogleCloudProject ID that contains Pub/Sub resources.
	GoogleCloudProject string

	// PubsubClientOptions is a list of options to pass to pubsub.NewClient. This may be useful to customize GRPC
	// behavior for example.
	PubsubClientOptions []option.ClientOption

	// Subscriptions is a list of all the Hedwig topics that the app is subscribed to (exclude the ``hedwig-`` prefix).
	// For subscribing to cross-project topic messages, use SubscriptionsCrossProject. Google only.
	Subscriptions []string

	// SubscriptionsCrossProject is a list of tuples of topic name and GCP project for cross-project topic messages.
	// Google only.
	SubscriptionsCrossProject []SubscriptionProject
}

func (b *Backend) initDefaults() {
	if b.settings.PubsubClientOptions == nil {
		b.settings.PubsubClientOptions = []option.ClientOption{}
	}
	if b.logger == nil {
		b.logger = &hedwig.StdLogger{}
	}
}

// NewBackend creates a Backend for publishing and consuming from GCP
// The provider metadata produced by this Backend will have concrete type: gcp.Metadata
func NewBackend(settings Settings, logger hedwig.Logger) *Backend {
	b := &Backend{settings: settings, logger: logger}
	b.initDefaults()
	return b
}
