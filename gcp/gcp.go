package gcp

import (
	"context"
	"fmt"
	"time"

	"golang.org/x/sync/errgroup"

	"cloud.google.com/go/pubsub"
	"github.com/pkg/errors"

	"github.com/cloudchacho/hedwig-go"
)

type gcpBackend struct {
	settings *hedwig.Settings
	client   *pubsub.Client
}

const defaultVisibilityTimeoutS = time.Second * 20

// GCPMetadata is additional metadata associated with a message
type GCPMetadata struct {
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
func (g *gcpBackend) Publish(ctx context.Context, message *hedwig.Message, payload []byte, attributes map[string]string, topic string) (string, error) {
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
	messageId, err := result.Get(ctx)
	if err != nil {
		return "", errors.Wrap(err, "Failed to publish message to SNS")
	}
	return messageId, nil
}

// Receive messages from configured queue(s) and provide it through the callback. This should run indefinitely
// until the context is cancelled. Provider metadata should include all info necessary to ack/nack a message.
func (g *gcpBackend) Receive(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration, callback hedwig.ConsumerCallback) error {
	err := g.ensureClient(ctx)
	if err != nil {
		return err
	}

	defer g.client.Close()

	subscriptionProjects := []hedwig.SubscriptionProject{}
	subscriptionProjects = append(subscriptionProjects, g.settings.SubscriptionsCrossProject...)
	for _, subscription := range g.settings.Subscriptions {
		subscriptionProjects = append(subscriptionProjects, hedwig.SubscriptionProject{subscription, g.settings.GoogleCloudProject})
	}
	subscriptionProjects = append(subscriptionProjects, hedwig.SubscriptionProject{g.settings.QueueName, g.settings.GoogleCloudProject})

	group, gctx := errgroup.WithContext(ctx)

	for _, subscriptionProject := range subscriptionProjects {
		pubsubSubscription := g.client.SubscriptionInProject(
			fmt.Sprintf("hedwig-%s", subscriptionProject.Subscription), subscriptionProject.ProjectID)
		pubsubSubscription.ReceiveSettings.MaxOutstandingMessages = int(numMessages)
		if visibilityTimeout != 0 {
			pubsubSubscription.ReceiveSettings.MaxExtensionPeriod = visibilityTimeout
		} else {
			pubsubSubscription.ReceiveSettings.MaxExtensionPeriod = defaultVisibilityTimeoutS
		}
		group.Go(func() error {
			err := pubsubSubscription.Receive(gctx, func(ctx context.Context, message *pubsub.Message) {
				metadata := GCPMetadata{
					pubsubMessage:   message,
					PublishTime:     message.PublishTime,
					DeliveryAttempt: *message.DeliveryAttempt,
				}
				callback(ctx, message.Data, message.Attributes, metadata)
			})
			return err
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
func (g *gcpBackend) NackMessage(ctx context.Context, providerMetadata interface{}) error {
	providerMetadata.(GCPMetadata).pubsubMessage.Nack()
	return nil
}

// AckMessage acknowledges a message on the queue
func (g *gcpBackend) AckMessage(ctx context.Context, providerMetadata interface{}) error {
	providerMetadata.(GCPMetadata).pubsubMessage.Ack()
	return nil
}

func (g *gcpBackend) ensureClient(ctx context.Context) error {
	if g.client != nil {
		return nil
	}
	client, err := pubsub.NewClient(context.Background(), g.settings.GoogleCloudProject)
	if err != nil {
		return err
	}
	g.client = client
	return nil
}

// NewGCPBackend creates a backend for publishing and consuming from GCP
// The provider metadata produced by this backend will have concrete type: gcp.GCPMetadata
func NewGCPBackend(settings *hedwig.Settings) hedwig.IBackend {
	return &gcpBackend{settings: settings}
}
