package gcp

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"
	"golang.org/x/oauth2/google"
	"golang.org/x/sync/errgroup"

	"github.com/cloudchacho/hedwig-go"
)

type backend struct {
	settings *hedwig.Settings
	client   *pubsub.Client
}

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
func (g *backend) Publish(ctx context.Context, message *hedwig.Message, payload []byte, attributes map[string]string, topic string) (string, error) {
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
func (g *backend) Receive(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration, callback hedwig.ConsumerCallback) error {
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

// NackMessage nacks a message on the queue
func (g *backend) NackMessage(ctx context.Context, providerMetadata interface{}) error {
	providerMetadata.(Metadata).pubsubMessage.Nack()
	return nil
}

// AckMessage acknowledges a message on the queue
func (g *backend) AckMessage(ctx context.Context, providerMetadata interface{}) error {
	providerMetadata.(Metadata).pubsubMessage.Ack()
	return nil
}

func (g *backend) ensureClient(ctx context.Context) error {
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
	client, err := pubsub.NewClient(context.Background(), googleCloudProject)
	if err != nil {
		return err
	}
	g.client = client
	return nil
}

// NewBackend creates a backend for publishing and consuming from GCP
// The provider metadata produced by this backend will have concrete type: gcp.Metadata
func NewBackend(settings *hedwig.Settings) hedwig.IBackend {
	return &backend{settings: settings}
}
