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

type awsBackend struct {
	settings *hedwig.Settings

	sqs sqsiface.SQSAPI
	sns snsiface.SNSAPI
}

// AWSMetadata is additional metadata associated with a message
type AWSMetadata struct {
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

func (a *awsBackend) getSQSQueueName() string {
	return fmt.Sprintf("HEDWIG-%s", a.settings.QueueName)
}

func (a *awsBackend) getSNSTopic(messageTopic string) string {
	return fmt.Sprintf("arn:aws:sns:%s:%s:hedwig-%s", a.settings.AWSRegion, a.settings.AWSAccountID, messageTopic)
}

func (a *awsBackend) getSQSQueueURL(ctx context.Context) (*string, error) {
	out, err := a.sqs.GetQueueUrlWithContext(ctx, &sqs.GetQueueUrlInput{
		QueueName: aws.String(a.getSQSQueueName()),
	})
	if err != nil {
		return nil, err
	}
	return out.QueueUrl, nil
}

// isValidForSQS checks that the payload is allowed in SQS message body since only some UTF8 characters are allowed
// ref: https://docs.amazonaws.cn/en_us/AWSSimpleQueueService/latest/APIReference/API_SendMessage.html
func (a *awsBackend) isValidForSQS(payload []byte) bool {
	if !utf8.Valid(payload) {
		return false
	}
	return bytes.IndexFunc(payload, func(r rune) bool {
		//  allowed characters: #x9 | #xA | #xD | #x20 to #xD7FF | #xE000 to #xFFFD | #x10000 to #x10FFFF
		return !(r == '\x09' || r == '\x0A' || r == '\x0D' || (r >= '\x20' && r <= '\uD7FF') || (r >= '\uE000' && r <= '\uFFFD') || (r >= '\U00010000' && r <= '\U0010FFFF'))
	}) == -1
}

// Publish a message represented by the payload, with specified attributes to the specific topic
func (a *awsBackend) Publish(ctx context.Context, message *hedwig.Message, payload []byte, attributes map[string]string, topic string) (string, error) {
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
// until the context is cancelled. Provider metadata should include all info necessary to ack/nack a message.
func (a *awsBackend) Receive(ctx context.Context, numMessages uint32, visibilityTimeoutS uint32, callback hedwig.ConsumerCallback) error {
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
	if visibilityTimeoutS != 0 {
		input.VisibilityTimeout = aws.Int64(int64(visibilityTimeoutS))
	}

	for {
		select {
		case <-ctx.Done():
			// if context was canceled, signal appropriately
			return ctx.Err()
		default:
			if deadline, ok := ctx.Deadline(); ok {
				// is shutting down?
				if time.Until(deadline) < a.settings.ShutdownTimeout {
					return errors.New("context shutting down")
				}
			}
			wg := sync.WaitGroup{}
			out, err := a.sqs.ReceiveMessageWithContext(ctx, input)
			if err != nil {
				return errors.Wrap(err, "failed to receive SQS message")
			}
			for i := range out.Messages {
				select {
				case <-ctx.Done():
					wg.Wait()
					// if context was canceled, signal appropriately
					return ctx.Err()
				default:
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
							firstReceiveTimestamp = 0
							firstReceiveTime = time.Time{}
						} else {
							firstReceiveTime = time.Unix(0, int64(time.Duration(firstReceiveTimestamp)*time.Millisecond)).UTC()
						}
						var sentTime time.Time
						if sentTimestamp, err := strconv.Atoi(*queueMessage.Attributes[sqs.MessageSystemAttributeNameSentTimestamp]); err != nil {
							sentTimestamp = 0
							sentTime = time.Time{}
						} else {
							sentTime = time.Unix(0, int64(time.Duration(sentTimestamp)*time.Millisecond)).UTC()
						}
						receiveCount, err := strconv.Atoi(*queueMessage.Attributes[sqs.MessageSystemAttributeNameApproximateReceiveCount])
						if err != nil {
							receiveCount = -1
						}
						metadata := AWSMetadata{
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
			}
			wg.Wait()
		}
	}
}

// NackMessage nacks a message on the queue
func (a *awsBackend) NackMessage(ctx context.Context, providerMetadata interface{}) error {
	// not supported by AWS
	return nil
}

// AckMessage acknowledges a message on the queue
func (a *awsBackend) AckMessage(ctx context.Context, providerMetadata interface{}) error {
	receipt := providerMetadata.(AWSMetadata).ReceiptHandle
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

// NewAWSBackend creates a backend for publishing and consuming from AWS
func NewAWSBackend(settings *hedwig.Settings, sessionCache *AWSSessionsCache) hedwig.IBackend {

	awsSession := sessionCache.GetSession(settings)

	return &awsBackend{
		settings,
		sqs.New(awsSession),
		sns.New(awsSession),
	}
}
