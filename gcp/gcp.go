package gcp

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
	"golang.org/x/sync/errgroup"

	"github.com/cloudchacho/hedwig-go"
)

type Backend struct {
	settings *hedwig.Settings
	client   *pubsub.Client
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
}

// Publish a message represented by the payload, with specified attributes to the specific topic
func (g *Backend) Publish(ctx context.Context, message *hedwig.Message, payload []byte, attributes map[string]string, topic string) (string, error) {
	err := g.ensureClient(ctx)
	if err != nil {
		return "", err
	}

	clientTopic := g.client.Topic(fmt.Sprintf("hedwig-%s", topic))
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
func (g *Backend) Receive(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration, callback hedwig.ConsumerCallback) error {
	err := g.ensureClient(ctx)
	if err != nil {
		return err
	}

	defer g.client.Close()

	subscriptions := []string{}
	// all subscriptions live in an app's project, but cross-project subscriptions are named differently
	for _, subscription := range g.settings.SubscriptionsCrossProject {
		subscriptionName := fmt.Sprintf("hedwig-%s-%s-%s", g.settings.QueueName, subscription.ProjectID, subscription.Subscription)
		subscriptions = append(subscriptions, subscriptionName)
	}
	for _, subscription := range g.settings.Subscriptions {
		subscriptionName := fmt.Sprintf("hedwig-%s-%s", g.settings.QueueName, subscription)
		subscriptions = append(subscriptions, subscriptionName)
	}
	// main queue for DLQ re-queued messages
	subscriptionName := fmt.Sprintf("hedwig-%s", g.settings.QueueName)
	subscriptions = append(subscriptions, subscriptionName)

	group, gctx := errgroup.WithContext(ctx)

	for _, subscription := range subscriptions {
		pubsubSubscription := g.client.Subscription(subscription)
		pubsubSubscription.ReceiveSettings.NumGoroutines = 1
		pubsubSubscription.ReceiveSettings.MaxOutstandingMessages = int(numMessages)
		if visibilityTimeout != 0 {
			pubsubSubscription.ReceiveSettings.MaxExtensionPeriod = visibilityTimeout
		} else {
			pubsubSubscription.ReceiveSettings.MaxExtensionPeriod = defaultVisibilityTimeoutS
		}
		group.Go(func() error {
			recvErr := pubsubSubscription.Receive(gctx, func(ctx context.Context, message *pubsub.Message) {
				metadata := Metadata{
					pubsubMessage:   message,
					PublishTime:     message.PublishTime,
					DeliveryAttempt: *message.DeliveryAttempt,
				}
				callback(ctx, message.Data, message.Attributes, metadata)
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
func (g *Backend) RequeueDLQ(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration) error {
	err := g.ensureClient(ctx)
	if err != nil {
		return err
	}

	defer g.client.Close()

	clientTopic := g.client.Topic(fmt.Sprintf("hedwig-%s", g.settings.QueueName))
	defer clientTopic.Stop()

	clientTopic.PublishSettings.CountThreshold = int(numMessages)
	if visibilityTimeout != 0 {
		clientTopic.PublishSettings.Timeout = visibilityTimeout
	} else {
		clientTopic.PublishSettings.Timeout = defaultVisibilityTimeoutS
	}

	pubsubSubscription := g.client.Subscription(fmt.Sprintf("hedwig-%s-dlq", g.settings.QueueName))
	pubsubSubscription.ReceiveSettings.MaxOutstandingMessages = int(numMessages)
	pubsubSubscription.ReceiveSettings.MaxExtensionPeriod = clientTopic.PublishSettings.Timeout

	// run a ticker that will fire after timeout and shutdown subscriber
	overallTimeout := time.Second * 5
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
				g.settings.GetLogger(ctx).Info("Re-queue DLQ progress", hedwig.LoggingFields{"num_messages": atomic.LoadUint32(&numMessagesRequeued)})
			case <-rctx.Done():
				return
			}
		}
	}()

	publishErrCh := make(chan error, 10)
	defer close(publishErrCh)
	err = pubsubSubscription.Receive(rctx, func(ctx context.Context, message *pubsub.Message) {
		ticker.Reset(overallTimeout)
		result := clientTopic.Publish(rctx, message)
		_, err := result.Get(rctx)
		if err != nil {
			message.Nack()
			cancel()
			publishErrCh <- err
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
	// context cancelation doesn't return error in Receive, don't return error from rctx since cancelation is happy
	// path
	return ctx.Err()
}

// NackMessage nacks a message on the queue
func (g *Backend) NackMessage(ctx context.Context, providerMetadata interface{}) error {
	providerMetadata.(Metadata).pubsubMessage.Nack()
	return nil
}

// AckMessage acknowledges a message on the queue
func (g *Backend) AckMessage(ctx context.Context, providerMetadata interface{}) error {
	providerMetadata.(Metadata).pubsubMessage.Ack()
	return nil
}

func (g *Backend) ensureClient(ctx context.Context) error {
	googleCloudProject := g.settings.GoogleCloudProject
	if googleCloudProject == "" {
		creds, err := google.FindDefaultCredentials(ctx)
		if err != nil {
			return errors.Wrap(
				err, "unable to discover google cloud project setting, either pass explicitly, or fix runtime environment")
		} else if creds.ProjectID == "" {
			return errors.New(
				"unable to discover google cloud project setting, either pass explicitly, or fix runtime environment")
		}
		googleCloudProject = creds.ProjectID
	}
	if g.client != nil {
		return nil
	}
	client, err := pubsub.NewClient(context.Background(), googleCloudProject, g.settings.PubsubClientOptions...)
	if err != nil {
		return err
	}
	g.client = client
	return nil
}

// NewBackend creates a Backend for publishing and consuming from GCP
// The provider metadata produced by this Backend will have concrete type: gcp.Metadata
func NewBackend(settings *hedwig.Settings) *Backend {
	return &Backend{settings: settings}
}
