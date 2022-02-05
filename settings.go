package hedwig

import (
	"context"
	"time"

	"google.golang.org/api/option"
)

// Settings for Hedwig
type Settings struct {
	// AWS Region
	AWSRegion string
	// AWS account id
	AWSAccountID string
	// AWS access key
	AWSAccessKey string
	// AWS secret key
	AWSSecretKey string
	// AWS session tokenthat represents temporary credentials (i.e. for Lambda app)
	AWSSessionToken string

	// AWS read timeout for Publisher
	AWSReadTimeoutS time.Duration // optional; default: 2 seconds

	// CallbackRegistry contains callbacks by message type and message version
	CallbackRegistry CallbackRegistry

	// DataFactoryRegistry contains data factories by message type and message version
	DataFactoryRegistry DataFactoryRegistry

	// GetLogger is a function that takes the context object and returns a logger. This may be used to plug in
	// your desired logger library. Defaults to using std library.
	// Convenience structs are provided for popular libraries: LogrusGetLoggerFunc
	GetLogger GetLoggerFunc

	// Maps message type and major version to topic names
	//   <message type>, <message version> => topic name
	// An entry is required for every message type that the app wants to Consumer or publish. It is
	// recommended that major versions of a message be published on separate topics.
	MessageRouting MessageRouting

	// PublisherName name
	PublisherName string

	// Hedwig queue name. Exclude the `HEDWIG-` prefix
	QueueName string

	// Subscriptions is a list of all the Hedwig topics that the app is subscribed to (exclude the ``hedwig-`` prefix).
	// For subscribing to cross-project topic messages, use SubscriptionsCrossProject. Google only.
	Subscriptions []string

	// SubscriptionsCrossProject is a list of tuples of topic name and GCP project for cross-project topic messages.
	// Google only.
	SubscriptionsCrossProject []SubscriptionProject

	// ShutdownTimeout is the time the app has to shut down before being brutally killed
	ShutdownTimeout time.Duration // optional; defaults to 10s

	// UseTransportMessageAttributes is a flag indicating if meta attributes should be sent as transport message
	// attributes. If set to False, meta attributes are sent as part of the payload - this is the legacy method for
	// publishing metadata and newer apps should not change this value.
	// default True
	UseTransportMessageAttributes *bool

	// GoogleCloudProject ID that contains Pub/Sub resources.
	GoogleCloudProject string

	// PubsubClientOptions is a list of options to pass to pubsub.NewClient. This may be useful to customize GRPC
	// behavior for example.
	PubsubClientOptions []option.ClientOption
}

func (s *Settings) initDefaults() {
	if s.AWSReadTimeoutS == 0 {
		s.AWSReadTimeoutS = 2 * time.Second
	}
	if s.ShutdownTimeout == 0 {
		s.ShutdownTimeout = 10 * time.Second
	}
	if s.GetLogger == nil {
		stdLogger := &stdLogger{}
		s.GetLogger = func(_ context.Context) ILogger { return stdLogger }
	}
	if s.UseTransportMessageAttributes == nil {
		useTransportMessageAttributes := true
		s.UseTransportMessageAttributes = &useTransportMessageAttributes
	}
	if s.PubsubClientOptions == nil {
		s.PubsubClientOptions = []option.ClientOption{}
	}
}
