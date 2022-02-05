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
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/pkg/errors"

	"github.com/cloudchacho/hedwig-go"
)

type Backend struct {
	settings *hedwig.Settings

	sqs sqsiface.SQSAPI
	sns snsiface.SNSAPI
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

func (a *Backend) getSQSQueueName() string {
	return fmt.Sprintf("HEDWIG-%s", a.settings.QueueName)
}

func (a *Backend) getSQSDLQName() string {
	return fmt.Sprintf("HEDWIG-%s-DLQ", a.settings.QueueName)
}

func (a *Backend) getSNSTopic(messageTopic string) string {
	return fmt.Sprintf("arn:aws:sns:%s:%s:hedwig-%s", a.settings.AWSRegion, a.settings.AWSAccountID, messageTopic)
}

func (a *Backend) getSQSQueueURL(ctx context.Context) (*string, error) {
	out, err := a.sqs.GetQueueUrlWithContext(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(a.getSQSQueueName()),
	})
	if err != nil {
		return nil, err
	}
	return out.QueueUrl, nil
}

func (a *Backend) getSQSDLQURL(ctx context.Context) (*string, error) {
	out, err := a.sqs.GetQueueUrlWithContext(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(a.getSQSDLQName()),
	})
	if err != nil {
		return nil, err
	}
	return out.QueueUrl, nil
}

// isValidForSQS checks that the payload is allowed in SQS message body since only some UTF8 characters are allowed
// ref: https://docs.amazonaws.cn/en_us/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html
func (a *Backend) isValidForSQS(payload []byte) bool {
	if !utf8.Valid(payload) {
		return false
	}
	return bytes.IndexFunc(payload, func(r rune) bool {
		//  allowed characters: #x9 | #xA | #xD | #x20 to #xD7FF | #xE000 to #xFFFD | #x10000 to #x10FFFF
		return !(r == '\x09' || r == '\x0A' || r == '\x0D' || (r >= '\x20' && r <= '\uD7FF') || (r >= '\uE000' && r <= '\uFFFD') || (r >= '\U00010000' && r <= '\U0010FFFF'))
	}) == -1
}

// Publish a message represented by the payload, with specified attributes to the specific topic
func (a *Backend) Publish(ctx context.Context, message *hedwig.Message, payload []byte, attributes map[string]string, topic string) (string, error) {
	snsTopic := a.getSNSTopic(topic)
	var payloadStr string

	// SNS requires UTF-8 encoded string
	if !a.isValidForSQS(payload) {
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

	result, err := a.sns.PublishWithContext(
		ctx,
		&sns.PublishInput{
			TopicArn:          &snsTopic,
			Message:           &payloadStr,
			MessageAttributes: snsAttributes,
		},
		request.WithResponseReadTimeout(a.settings.AWSReadTimeoutS),
	)
	if err != nil {
		return "", errors.Wrap(err, "Failed to publish message to SNS")
	}
	return *result.MessageId, nil
}

// Receive messages from configured queue(s) and provide it through the callback. This should run indefinitely
// until the context is canceled. Provider metadata should include all info necessary to ack/nack a message.
func (a *Backend) Receive(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration, callback hedwig.ConsumerCallback) error {
	queueURL, err := a.getSQSQueueURL(ctx)
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
		out, err := a.sqs.ReceiveMessageWithContext(ctx, input)
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
						a.settings.GetLogger(ctx).Error(
							err,
							"Invalid message payload - couldn't decode using base64",
							hedwig.LoggingFields{"message_id": queueMessage.MessageId},
						)
						return
					}
				}
				callback(ctx, payload, attributes, metadata)
			}()
		}
		wg.Wait()
	}
}

// RequeueDLQ re-queues everything in the Hedwig DLQ back into the Hedwig queue
func (a *Backend) RequeueDLQ(ctx context.Context, numMessages uint32, visibilityTimeout time.Duration) error {
	queueURL, err := a.getSQSQueueURL(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get SQS Queue URL")
	}
	dlqURL, err := a.getSQSDLQURL(ctx)
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
		out, err := a.sqs.ReceiveMessageWithContext(ctx, input)
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
		sendOut, err := a.sqs.SendMessageBatchWithContext(ctx, sendInput, request.WithResponseReadTimeout(a.settings.AWSReadTimeoutS))
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
			deleteOutput, err := a.sqs.DeleteMessageBatchWithContext(ctx, deleteInput)
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
		a.settings.GetLogger(ctx).Info("Re-queue DLQ progress", hedwig.LoggingFields{"num_messages": numMessagesRequeued})
	}
}

// NackMessage nacks a message on the queue
func (a *Backend) NackMessage(ctx context.Context, providerMetadata interface{}) error {
	// not supported by AWS
	return nil
}

// AckMessage acknowledges a message on the queue
func (a *Backend) AckMessage(ctx context.Context, providerMetadata interface{}) error {
	receipt := providerMetadata.(Metadata).ReceiptHandle
	queueURL, err := a.getSQSQueueURL(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to get SQS Queue URL")
	}
	_, err = a.sqs.DeleteMessageWithContext(ctx, &sqs.DeleteMessageInput{
		QueueUrl:      queueURL,
		ReceiptHandle: aws.String(receipt),
	})
	return err
}

// NewBackend creates a Backend for publishing and consuming from AWS
// The provider metadata produced by this Backend will have concrete type: aws.Metadata
func NewBackend(settings *hedwig.Settings, sessionCache *SessionsCache) *Backend {

	awsSession := sessionCache.GetSession(settings)

	return &Backend{
		settings,
		sqs.New(awsSession),
		sns.New(awsSession),
	}
}
