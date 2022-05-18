package aws

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"strconv"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/pkg/errors"

	"github.com/cloudchacho/hedwig-go"
)

type Backend struct {
	settings Settings

	sqs    sqsiface.SQSAPI
	sns    snsiface.SNSAPI
	logger hedwig.Logger
}

var _ = hedwig.ConsumerBackend(&Backend{})
var _ = hedwig.PublisherBackend(&Backend{})

// Metadata is additional metadata associated with a message
type Metadata struct {
	// AWS receipt identifier
	ReceiptHandle string

	// FirstReceiveTime is time the message was first received from the queue. The value
	//    is calculated as best effort and is approximate.
	FirstReceiveTime time.Time

	// SentTime when this message was originally sent to AWS
	SentTime time.Time

	// ReceiveCount received from SQS.
	//    The first delivery of a given message will have this value as 1. The value
	//    is calculated as best effort and is approximate.
	ReceiveCount int
}

const sqsWaitTimeoutSeconds int64 = 20

func (b *Backend) getSQSQueueName() string {
	return fmt.Sprintf("HEDWIG-%s", b.settings.QueueName)
}

func (b *Backend) getSQSDLQName() string {
	return fmt.Sprintf("HEDWIG-%s-DLQ", b.settings.QueueName)
}

func (b *Backend) getSNSTopic(messageTopic string) string {
	return fmt.Sprintf("arn:aws:sns:%s:%s:hedwig-%s", b.settings.AWSRegion, b.settings.AWSAccountID, messageTopic)
}

func (b *Backend) getSQSQueueURL(ctx context.Context) (*string, error) {
	out, err := b.sqs.GetQueueUrlWithContext(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(b.getSQSQueueName()),
	})
	if err != nil {
		return nil, err
	}
	return out.QueueUrl, nil
}

func (b *Backend) getSQSDLQURL(ctx context.Context) (*string, error) {
	out, err := b.sqs.GetQueueUrlWithContext(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(b.getSQSDLQName()),
	})
	if err != nil {
		return nil, err
	}
	return out.QueueUrl, nil
}

// isValidForSQS checks that the payload is allowed in SQS message body since only some UTF8 characters are allowed
// ref: https://docs.amazonaws.cn/en_us/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html
func (b *Backend) isValidForSQS(payload []byte) bool {
	if !utf8.Valid(payload) {
		return false
	}
	return bytes.IndexFunc(payload, func(r rune) bool {
		//  allowed characters: #x9 | #xA | #xD | #x20 to #xD7FF | #xE000 to #xFFFD | #x10000 to #x10FFFF
		return !(r == '\x09' || r == '\x0A' || r == '\x0D' || (r >= '\x20' && r <= '\uD7FF') || (r >= '\uE000' && r <= '\uFFFD') || (r >= '\U00010000' && r <= '\U0010FFFF'))
	}) == -1
}

// Publish a message represented by the payload, with specified attributes to the specific topic
func (b *Backend) Publish(ctx context.Context, message *hedwig.Message, payload []byte, attributes map[string]string, topic string) (string, error) {
	snsTopic := b.getSNSTopic(topic)
	var payloadStr string

	// SNS requires UTF-8 encoded string
	if !b.isValidForSQS(payload) {
		payloadStr = base64.StdEncoding.EncodeToString(payload)
		attributes["hedwig_encoding"] = "base64"
	} else {
		payloadStr = string(payload)
	}

	snsAttributes := make(map[string]*sns.MessageAttributeValue)
	for key, value := range attributes {
		snsAttributes[key] = &sns.MessageAttributeValue{
			StringValue: aws.String(value),
			DataType:    aws.String("String"),
		}
	}

	result, err := b.sns.PublishWithContext(
		ctx,
		&sns.PublishInput{
			TopicArn:          &snsTopic,
			Message:           &payloadStr,
			MessageAttributes: snsAttributes,
		},
		request.WithResponseReadTimeout(b.settings.AWSReadTimeoutS),
	)
	if err != nil {
		return "", errors.Wrap(err, "Failed to publish message to SNS")
	}
	return *result.MessageId, nil
}

// Receive messages from configured queue(s) and provide it through the callback. This should run indefinitely
// until the context is canceled. Provider metadata should include all info necessary to ack/nack a message.
func (b *Backend) Receive(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration, messageCh chan<- hedwig.ReceivedMessage) error {
	queueURL, err := b.getSQSQueueURL(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get SQS Queue URL")
	}
	input := &sqs.ReceiveMessageInput{
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		QueueUrl:              queueURL,
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
	}
	if visibilityTimeout != 0 {
		input.VisibilityTimeout = aws.Int64(int64(visibilityTimeout.Seconds()))
	}

	for {
		if ctx.Err() != nil {
			// if work was canceled because of context cancelation, signal that
			return ctx.Err()
		}
		out, err := b.sqs.ReceiveMessageWithContext(ctx, input)
		if err != nil {
			return errors.Wrap(err, "failed to receive SQS message")
		}
		wg := sync.WaitGroup{}
		for i := range out.Messages {
			if ctx.Err() != nil {
				break
			}
			wg.Add(1)
			queueMessage := out.Messages[i]
			go func() {
				defer wg.Done()
				attributes := map[string]string{}
				for k, v := range queueMessage.MessageAttributes {
					attributes[k] = *v.StringValue
				}
				var firstReceiveTime time.Time
				if firstReceiveTimestamp, err := strconv.Atoi(*queueMessage.Attributes[sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp]); err != nil {
					firstReceiveTime = time.Time{}
				} else {
					firstReceiveTime = time.Unix(0, int64(time.Duration(firstReceiveTimestamp)*time.Millisecond)).UTC()
				}
				var sentTime time.Time
				if sentTimestamp, err := strconv.Atoi(*queueMessage.Attributes[sqs.MessageSystemAttributeNameSentTimestamp]); err != nil {
					sentTime = time.Time{}
				} else {
					sentTime = time.Unix(0, int64(time.Duration(sentTimestamp)*time.Millisecond)).UTC()
				}
				receiveCount, err := strconv.Atoi(*queueMessage.Attributes[sqs.MessageSystemAttributeNameApproximateReceiveCount])
				if err != nil {
					receiveCount = -1
				}
				metadata := Metadata{
					*queueMessage.ReceiptHandle,
					firstReceiveTime,
					sentTime,
					receiveCount,
				}
				payload := []byte(*queueMessage.Body)
				if encoding, ok := attributes["hedwig_encoding"]; ok && encoding == "base64" {
					payload, err = base64.StdEncoding.DecodeString(string(payload))
					if err != nil {
						b.logger.Error(
							ctx,
							err,
							"Invalid message payload - couldn't decode using base64",
							"message_id",
							queueMessage.MessageId,
						)
						return
					}
				}
				messageCh <- hedwig.ReceivedMessage{
					Payload:          payload,
					Attributes:       attributes,
					ProviderMetadata: metadata,
				}
			}()
		}
		wg.Wait()
	}
}

// RequeueDLQ re-queues everything in the Hedwig DLQ back into the Hedwig queue
func (b *Backend) RequeueDLQ(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration) error {
	queueURL, err := b.getSQSQueueURL(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get SQS Queue URL")
	}
	dlqURL, err := b.getSQSDLQURL(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get SQS DLQ URL")
	}
	input := &sqs.ReceiveMessageInput{
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		QueueUrl:              dlqURL,
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
	}
	if visibilityTimeout != 0 {
		input.VisibilityTimeout = aws.Int64(int64(visibilityTimeout.Seconds()))
	}
	var numMessagesRequeued uint32

	for {
		// if work was canceled because of context cancelation, signal that
		if ctx.Err() != nil {
			return ctx.Err()
		}
		out, err := b.sqs.ReceiveMessageWithContext(ctx, input)
		if err != nil {
			return errors.Wrap(err, "failed to receive SQS message")
		}
		if len(out.Messages) == 0 {
			return nil
		}
		receipts := make(map[string]*string, len(out.Messages))
		entries := make([]*sqs.SendMessageBatchRequestEntry, len(out.Messages))
		for i, message := range out.Messages {
			entries[i] = &sqs.SendMessageBatchRequestEntry{
				Id:                aws.String(*message.MessageId),
				MessageAttributes: message.MessageAttributes,
				MessageBody:       aws.String(*message.Body),
			}
			receipts[*message.MessageId] = message.ReceiptHandle
		}
		sendInput := &sqs.SendMessageBatchInput{Entries: entries, QueueUrl: queueURL}
		sendOut, err := b.sqs.SendMessageBatchWithContext(ctx, sendInput, request.WithResponseReadTimeout(b.settings.AWSReadTimeoutS))
		if err != nil {
			return errors.Wrap(err, "failed to send messages")
		}
		if len(sendOut.Successful) > 0 {
			deleteEntries := make([]*sqs.DeleteMessageBatchRequestEntry, len(sendOut.Successful))
			for i, successful := range sendOut.Successful {
				deleteEntries[i] = &sqs.DeleteMessageBatchRequestEntry{
					Id:            successful.Id,
					ReceiptHandle: receipts[*successful.Id],
				}
			}
			deleteInput := &sqs.DeleteMessageBatchInput{Entries: deleteEntries, QueueUrl: dlqURL}
			deleteOutput, err := b.sqs.DeleteMessageBatchWithContext(ctx, deleteInput)
			if err != nil {
				return errors.Wrap(err, "failed to ack messages")
			}
			if len(deleteOutput.Failed) > 0 {
				return errors.New("failed to ack some messages")
			}
		}
		if len(sendOut.Failed) > 0 {
			return errors.New("failed to send some messages")
		}
		numMessagesRequeued += uint32(len(sendOut.Successful))
		b.logger.Debug(ctx, "Re-queue DLQ progress", "num_messages", numMessagesRequeued)
	}
}

// NackMessage nacks a message on the queue
func (b *Backend) NackMessage(ctx context.Context, providerMetadata interface{}) error {
	// not supported by AWS
	return nil
}

// AckMessage acknowledges a message on the queue
func (b *Backend) AckMessage(ctx context.Context, providerMetadata interface{}) error {
	receipt := providerMetadata.(Metadata).ReceiptHandle
	queueURL, err := b.getSQSQueueURL(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get SQS Queue URL")
	}
	_, err = b.sqs.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      queueURL,
		ReceiptHandle: aws.String(receipt),
	})
	return err
}

// Settings for AWS Backend
type Settings struct {
	// AWS Region
	AWSRegion string
	// AWS account id
	AWSAccountID string
	// AWS access key
	AWSAccessKey string
	// AWS secret key
	AWSSecretKey string
	// AWS session token that represents temporary credentials (i.e. for Lambda app)
	AWSSessionToken string
	// AWS read timeout for Publisher
	AWSReadTimeoutS time.Duration // optional; default: 2 seconds
	// Name of the queue for this application
	QueueName string
}

func (b *Backend) initDefaults() {
	if b.settings.AWSReadTimeoutS == 0 {
		b.settings.AWSReadTimeoutS = 2 * time.Second
	}
	if b.logger == nil {
		b.logger = hedwig.StdLogger{}
	}
}

func createSession(region, awsAccessKey, awsSecretAccessKey, awsSessionToken string) *session.Session {
	var creds *credentials.Credentials
	if awsAccessKey != "" && awsSecretAccessKey != "" {
		creds = credentials.NewStaticCredentialsFromCreds(
			credentials.Value{
				AccessKeyID:     awsAccessKey,
				SecretAccessKey: awsSecretAccessKey,
				SessionToken:    awsSessionToken,
			},
		)
	}
	return session.Must(session.NewSessionWithOptions(
		session.Options{
			Config: aws.Config{
				Credentials: creds,
				Region:      aws.String(region),
				DisableSSL:  aws.Bool(false),
			},
		}))
}

// NewBackend creates a Backend for publishing and consuming from AWS
// The provider metadata produced by this Backend will have concrete type: aws.Metadata
func NewBackend(settings Settings, logger hedwig.Logger) *Backend {
	awsSession := createSession(
		settings.AWSRegion, settings.AWSAccessKey, settings.AWSSecretKey, settings.AWSSessionToken,
	)

	b := &Backend{
		settings: settings,
		sqs:      sqs.New(awsSession),
		sns:      sns.New(awsSession),
		logger:   logger,
	}
	b.initDefaults()
	return b
}
