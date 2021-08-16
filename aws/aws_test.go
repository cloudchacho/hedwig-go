package aws

import (
	"context"
	"encoding/base64"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cloudchacho/hedwig-go/internal/testutils"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/sns"
	"github.com/aws/aws-sdk-go/service/sns/snsiface"
	"github.com/aws/aws-sdk-go/service/sqs"
	"github.com/aws/aws-sdk-go/service/sqs/sqsiface"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/cloudchacho/hedwig-go"
)

type fakeHedwigDataField struct {
	VehicleID string `json:"vehicle_id"`
}

type fakeLog struct {
	level   string
	err     error
	message string
	fields  hedwig.LoggingFields
}

type fakeLogger struct {
	lock sync.Mutex
	logs []fakeLog
}

func (f *fakeLogger) Error(err error, message string, fields hedwig.LoggingFields) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"error", err, message, fields})
}

func (f *fakeLogger) Warn(err error, message string, fields hedwig.LoggingFields) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"warn", err, message, fields})
}

func (f *fakeLogger) Info(message string, fields hedwig.LoggingFields) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"info", nil, message, fields})
}

func (f *fakeLogger) Debug(message string, fields hedwig.LoggingFields) {
	f.lock.Lock()
	defer f.lock.Unlock()
	f.logs = append(f.logs, fakeLog{"debug", nil, message, fields})
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

type fakeSQS struct {
	mock.Mock
	// fake interface here
	sqsiface.SQSAPI
}

func (fs *fakeSQS) SendMessageWithContext(ctx aws.Context, in *sqs.SendMessageInput, opts ...request.Option) (*sqs.SendMessageOutput, error) {
	args := fs.Called(ctx, in, opts)
	return args.Get(0).(*sqs.SendMessageOutput), args.Error(1)
}

// revive:disable:var-naming
func (fs *fakeSQS) GetQueueUrlWithContext(ctx aws.Context, in *sqs.GetQueueUrlInput, opts ...request.Option) (*sqs.GetQueueUrlOutput, error) {
	args := fs.Called(ctx, in, opts)
	return args.Get(0).(*sqs.GetQueueUrlOutput), args.Error(1)
}

// revive:enable:var-naming

func (fs *fakeSQS) ReceiveMessageWithContext(ctx aws.Context, in *sqs.ReceiveMessageInput, opts ...request.Option) (*sqs.ReceiveMessageOutput, error) {
	args := fs.Called(ctx, in, opts)
	return args.Get(0).(*sqs.ReceiveMessageOutput), args.Error(1)
}

func (fs *fakeSQS) DeleteMessageWithContext(ctx aws.Context, in *sqs.DeleteMessageInput, opts ...request.Option) (*sqs.DeleteMessageOutput, error) {
	args := fs.Called(ctx, in, opts)
	return args.Get(0).(*sqs.DeleteMessageOutput), args.Error(1)
}

func (fs *fakeSQS) DeleteMessageBatchWithContext(ctx aws.Context, in *sqs.DeleteMessageBatchInput, opts ...request.Option) (*sqs.DeleteMessageBatchOutput, error) {
	args := fs.Called(ctx, in, opts)
	return args.Get(0).(*sqs.DeleteMessageBatchOutput), args.Error(1)
}

func (fs *fakeSQS) SendMessageBatchWithContext(ctx aws.Context, in *sqs.SendMessageBatchInput, opts ...request.Option) (*sqs.SendMessageBatchOutput, error) {
	args := fs.Called(ctx, in, opts)
	return args.Get(0).(*sqs.SendMessageBatchOutput), args.Error(1)
}

type fakeSNS struct {
	mock.Mock
	// fake interface here
	snsiface.SNSAPI
}

func (fs *fakeSNS) PublishWithContext(ctx aws.Context, in *sns.PublishInput, opts ...request.Option) (*sns.PublishOutput, error) {
	args := fs.Called(ctx, in)
	return args.Get(0).(*sns.PublishOutput), args.Error(1)
}

func (s *BackendTestSuite) TestReceive() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).Return(output, nil)

	receiptHandle := "foobar"
	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	firstReceiveTime, err := time.Parse(time.RFC3339Nano, "2011-01-19T22:15:10.456000000-07:00")
	s.Require().NoError(err)
	sentTime, err := time.Parse(time.RFC3339Nano, "2011-01-19T22:15:10.123000000-07:00")
	s.Require().NoError(err)
	receiveCount := 1
	body := `{"vehicle_id": "C_123"}`
	messageID := "123"
	sqsMessage := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo": {StringValue: aws.String("bar")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String("1295500510456"),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String("1295500510123"),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(strconv.Itoa(int(receiveCount))),
		},
		Body:      aws.String(body),
		MessageId: aws.String(messageID),
	}
	body2 := `vbI9vCDijJg=`
	messageID2 := "456"
	sqsMessage2 := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo":             {StringValue: aws.String("bar")},
			"hedwig_encoding": {StringValue: aws.String("base64")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String("1295500510456"),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String("1295500510123"),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(strconv.Itoa(int(receiveCount))),
		},
		Body:      aws.String(body2),
		MessageId: aws.String(messageID2),
	}
	receiveOutput := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{&sqsMessage, &sqsMessage2},
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(receiveOutput, nil).
		Once()
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil)

	payload := []byte(`{"vehicle_id": "C_123"}`)
	attributes := map[string]string{
		"foo": "bar",
	}
	providerMetadata := Metadata{
		ReceiptHandle:    receiptHandle,
		FirstReceiveTime: firstReceiveTime.UTC(),
		SentTime:         sentTime.UTC(),
		ReceiveCount:     receiveCount,
	}
	s.fakeConsumerCallback.On("Callback", mock.AnythingOfType("*context.timerCtx"), payload, attributes, providerMetadata).
		Return().
		Once()
	payload2 := []byte("\xbd\xb2\x3d\xbc\x20\xe2\x8c\x98")
	attributes2 := map[string]string{
		"foo":             "bar",
		"hedwig_encoding": "base64",
	}
	s.fakeConsumerCallback.On("Callback", mock.AnythingOfType("*context.timerCtx"), payload2, attributes2, providerMetadata).
		Return().
		Once().
		// force method to return after just one loop
		After(time.Millisecond * 11)

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*1)
	defer cancel()

	testutils.RunAndWait(func() {
		err := s.backend.Receive(ctx, numMessages, visibilityTimeout, s.fakeConsumerCallback.Callback)
		s.EqualError(err, "context deadline exceeded")
	})

	s.fakeSQS.AssertExpectations(s.T())
	s.fakeConsumerCallback.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestReceiveFailedNonUTF8Decoding() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).Return(output, nil)

	receiptHandle := "foobar"
	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	receiveCount := 1
	body := `foobar`
	messageID := "123"
	sqsMessage := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo":             {StringValue: aws.String("bar")},
			"hedwig_encoding": {StringValue: aws.String("base64")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String("1295500510456"),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String("1295500510123"),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(strconv.Itoa(int(receiveCount))),
		},
		Body:      aws.String(body),
		MessageId: aws.String(messageID),
	}
	receiveOutput := &sqs.ReceiveMessageOutput{Messages: []*sqs.Message{&sqsMessage}}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(receiveOutput, nil).
		Once()
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil)

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*1)
	defer cancel()

	testutils.RunAndWait(func() {
		err := s.backend.Receive(ctx, numMessages, visibilityTimeout, s.fakeConsumerCallback.Callback)
		s.EqualError(err, "context deadline exceeded")
	})

	s.fakeSQS.AssertExpectations(s.T())
	s.fakeConsumerCallback.AssertExpectations(s.T())
	s.Equal(len(s.logger.logs), 1)
	s.Equal(s.logger.logs[0].message, "Invalid message payload - couldn't decode using base64")
	s.Error(s.logger.logs[0].err)
}

func (s *BackendTestSuite) TestReceiveNoMessages() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).Return(output, nil)

	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil)

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*1)
	defer cancel()

	testutils.RunAndWait(func() {
		err := s.backend.Receive(ctx, numMessages, visibilityTimeout, s.fakeConsumerCallback.Callback)
		s.EqualError(err, "context deadline exceeded")
	})

	s.fakeSQS.AssertExpectations(s.T())
	s.fakeConsumerCallback.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestReceiveError() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).Return(output, nil)

	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, errors.New("no internet"))

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*1)
	defer cancel()

	testutils.RunAndWait(func() {
		err := s.backend.Receive(ctx, numMessages, visibilityTimeout, s.fakeConsumerCallback.Callback)
		s.EqualError(err, "failed to receive SQS message: no internet")
	})

	s.fakeSQS.AssertExpectations(s.T())
	s.fakeConsumerCallback.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestReceiveGetQueueError() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "HEDWIG-DEV-MYAPP"
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).
		Return((*sqs.GetQueueUrlOutput)(nil), errors.New("no internet"))

	err := s.backend.Receive(ctx, numMessages, visibilityTimeout, s.fakeConsumerCallback.Callback)
	s.EqualError(err, "failed to get SQS Queue URL: no internet")

	s.fakeSQS.AssertExpectations(s.T())
	s.fakeConsumerCallback.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestReceiveMissingAttributes() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).Return(output, nil)

	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(queueURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	receiptHandle := "123"
	body := `{"vehicle_id": "C_123"}`
	messageID := "123"
	sqsMessage := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo": {StringValue: aws.String("bar")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String(""),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String(""),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(""),
		},
		Body:      aws.String(body),
		MessageId: aws.String(messageID),
	}
	receiveOutput := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{&sqsMessage},
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(receiveOutput, nil).
		Once()
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil)

	payload := []byte(`{"vehicle_id": "C_123"}`)
	attributes := map[string]string{
		"foo": "bar",
	}
	providerMetadata := Metadata{
		ReceiptHandle:    receiptHandle,
		FirstReceiveTime: time.Time{},
		SentTime:         time.Time{},
		ReceiveCount:     -1,
	}
	s.fakeConsumerCallback.On("Callback", mock.AnythingOfType("*context.timerCtx"), payload, attributes, providerMetadata).
		Return().
		Once()

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*1)
	defer cancel()
	testutils.RunAndWait(func() {
		err := s.backend.Receive(ctx, numMessages, visibilityTimeout, s.fakeConsumerCallback.Callback)
		s.EqualError(err, "context deadline exceeded")
	})

	s.fakeSQS.AssertExpectations(s.T())
	s.fakeConsumerCallback.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestRequeueDLQ() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10

	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).
		Return(output, nil).
		Once()

	dlqName := "HEDWIG-DEV-MYAPP-DLQ"
	dlqURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + dlqName
	queueInput = &sqs.GetQueueUrlInput{
		QueueName: &dlqName,
	}
	output = &sqs.GetQueueUrlOutput{
		QueueUrl: &dlqURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).
		Return(output, nil).
		Once()

	receiptHandle := "foobar"
	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(dlqURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	receiveCount := 1
	body := `{"vehicle_id": "C_123"}`
	messageID := "123"
	sqsMessage := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo": {StringValue: aws.String("bar")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String("1295500510456"),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String("1295500510123"),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(strconv.Itoa(int(receiveCount))),
		},
		Body:      aws.String(body),
		MessageId: aws.String(messageID),
	}
	receiptHandle2 := "foobar2"
	body2 := `vbI9vCDijJg=`
	messageID2 := "456"
	sqsMessage2 := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle2),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo":             {StringValue: aws.String("bar")},
			"hedwig_encoding": {StringValue: aws.String("base64")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String("1295500510456"),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String("1295500510123"),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(strconv.Itoa(int(receiveCount))),
		},
		Body:      aws.String(body2),
		MessageId: aws.String(messageID2),
	}
	receiveOutput := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{&sqsMessage, &sqsMessage2},
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(receiveOutput, nil).
		Once().
		After(time.Millisecond * 10)
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil)

	sendInput1 := sqs.SendMessageBatchRequestEntry{
		Id:                aws.String(messageID),
		MessageAttributes: sqsMessage.MessageAttributes,
		MessageBody:       aws.String(body),
	}
	sendInput2 := sqs.SendMessageBatchRequestEntry{
		Id:                aws.String(messageID2),
		MessageAttributes: sqsMessage2.MessageAttributes,
		MessageBody:       aws.String(body2),
	}
	sendInput := &sqs.SendMessageBatchInput{
		Entries:  []*sqs.SendMessageBatchRequestEntry{&sendInput1, &sendInput2},
		QueueUrl: aws.String(queueURL),
	}
	sendOutput := &sqs.SendMessageBatchOutput{
		Failed: []*sqs.BatchResultErrorEntry{},
		Successful: []*sqs.SendMessageBatchResultEntry{
			{Id: aws.String(messageID)},
			{Id: aws.String(messageID2)},
		},
	}
	s.fakeSQS.On("SendMessageBatchWithContext", mock.AnythingOfType("*context.timerCtx"), sendInput, mock.Anything).
		Return(sendOutput, nil).
		Once()

	deleteInput := &sqs.DeleteMessageBatchInput{
		Entries: []*sqs.DeleteMessageBatchRequestEntry{
			{
				Id:            aws.String(messageID),
				ReceiptHandle: aws.String(receiptHandle),
			},
			{
				Id:            aws.String(messageID2),
				ReceiptHandle: aws.String(receiptHandle2),
			},
		},
		QueueUrl: aws.String(dlqURL),
	}
	deleteOutput := &sqs.DeleteMessageBatchOutput{
		Failed: nil,
		Successful: []*sqs.DeleteMessageBatchResultEntry{
			{Id: aws.String(messageID)},
			{Id: aws.String(messageID2)},
		},
	}

	s.fakeSQS.On("DeleteMessageBatchWithContext", mock.AnythingOfType("*context.timerCtx"), deleteInput, []request.Option(nil)).
		Return(deleteOutput, nil).
		Once()

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*1)
	defer cancel()

	testutils.RunAndWait(func() {
		err := s.backend.RequeueDLQ(ctx, numMessages, visibilityTimeout)
		s.EqualError(err, "context deadline exceeded")
	})

	s.fakeSQS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestRequeueDLQNoMessages() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).
		Return(output, nil).
		Once()

	dlqName := "HEDWIG-DEV-MYAPP-DLQ"
	dlqURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + dlqName
	queueInput = &sqs.GetQueueUrlInput{
		QueueName: &dlqName,
	}
	output = &sqs.GetQueueUrlOutput{
		QueueUrl: &dlqURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).
		Return(output, nil).
		Once()

	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(dlqURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil)

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*1)
	defer cancel()

	testutils.RunAndWait(func() {
		err := s.backend.RequeueDLQ(ctx, numMessages, visibilityTimeout)
		s.NoError(err)
	})

	s.fakeSQS.AssertExpectations(s.T())
	s.fakeConsumerCallback.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestRequeueDLQReceiveError() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).Return(output, nil)

	dlqName := "HEDWIG-DEV-MYAPP-DLQ"
	dlqURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + dlqName
	queueInput = &sqs.GetQueueUrlInput{
		QueueName: &dlqName,
	}
	output = &sqs.GetQueueUrlOutput{
		QueueUrl: &dlqURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).
		Return(output, nil).
		Once()

	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(dlqURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, errors.New("no internet"))

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*1)
	defer cancel()

	testutils.RunAndWait(func() {
		err := s.backend.RequeueDLQ(ctx, numMessages, visibilityTimeout)
		s.EqualError(err, "failed to receive SQS message: no internet")
	})

	s.fakeSQS.AssertExpectations(s.T())
	s.fakeConsumerCallback.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestRequeueDLQPublishError() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).Return(output, nil)

	dlqName := "HEDWIG-DEV-MYAPP-DLQ"
	dlqURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + dlqName
	queueInput = &sqs.GetQueueUrlInput{
		QueueName: &dlqName,
	}
	output = &sqs.GetQueueUrlOutput{
		QueueUrl: &dlqURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).
		Return(output, nil).
		Once()

	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(dlqURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	receiveCount := 1
	body := `{"vehicle_id": "C_123"}`
	messageID := "123"
	receiptHandle := "foobar"
	sqsMessage := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo": {StringValue: aws.String("bar")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String("1295500510456"),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String("1295500510123"),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(strconv.Itoa(int(receiveCount))),
		},
		Body:      aws.String(body),
		MessageId: aws.String(messageID),
	}
	receiveOutput := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{&sqsMessage},
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(receiveOutput, nil).
		Once().
		After(time.Millisecond * 10)
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil)

	sendInput1 := sqs.SendMessageBatchRequestEntry{
		Id:                aws.String(messageID),
		MessageAttributes: sqsMessage.MessageAttributes,
		MessageBody:       aws.String(body),
	}
	sendInput := &sqs.SendMessageBatchInput{
		Entries:  []*sqs.SendMessageBatchRequestEntry{&sendInput1},
		QueueUrl: aws.String(queueURL),
	}
	s.fakeSQS.On("SendMessageBatchWithContext", mock.AnythingOfType("*context.timerCtx"), sendInput, mock.Anything).
		Return((*sqs.SendMessageBatchOutput)(nil), errors.New("no internet"))

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*1)
	defer cancel()

	testutils.RunAndWait(func() {
		err := s.backend.RequeueDLQ(ctx, numMessages, visibilityTimeout)
		s.EqualError(err, "failed to send messages: no internet")
	})

	s.fakeSQS.AssertExpectations(s.T())
	s.fakeConsumerCallback.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestRequeueDLQPublishPartialError() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).Return(output, nil)

	dlqName := "HEDWIG-DEV-MYAPP-DLQ"
	dlqURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + dlqName
	queueInput = &sqs.GetQueueUrlInput{
		QueueName: &dlqName,
	}
	output = &sqs.GetQueueUrlOutput{
		QueueUrl: &dlqURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).
		Return(output, nil).
		Once()

	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(dlqURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	receiveCount := 1
	body := `{"vehicle_id": "C_123"}`
	messageID := "123"
	receiptHandle := "foobar"
	sqsMessage := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo": {StringValue: aws.String("bar")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String("1295500510456"),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String("1295500510123"),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(strconv.Itoa(int(receiveCount))),
		},
		Body:      aws.String(body),
		MessageId: aws.String(messageID),
	}
	receiveOutput := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{&sqsMessage},
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(receiveOutput, nil).
		Once().
		After(time.Millisecond * 10)
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil)

	sendInput1 := sqs.SendMessageBatchRequestEntry{
		Id:                aws.String(messageID),
		MessageAttributes: sqsMessage.MessageAttributes,
		MessageBody:       aws.String(body),
	}
	sendInput := &sqs.SendMessageBatchInput{
		Entries:  []*sqs.SendMessageBatchRequestEntry{&sendInput1},
		QueueUrl: aws.String(queueURL),
	}
	sendOutput := &sqs.SendMessageBatchOutput{
		Failed: []*sqs.BatchResultErrorEntry{
			{Id: aws.String(messageID)},
		},
		Successful: []*sqs.SendMessageBatchResultEntry{},
	}
	s.fakeSQS.On("SendMessageBatchWithContext", mock.AnythingOfType("*context.timerCtx"), sendInput, mock.Anything).
		Return(sendOutput, nil)

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*1)
	defer cancel()

	testutils.RunAndWait(func() {
		err := s.backend.RequeueDLQ(ctx, numMessages, visibilityTimeout)
		s.EqualError(err, "failed to send some messages")
	})

	s.fakeSQS.AssertExpectations(s.T())
	s.fakeConsumerCallback.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestRequeueDLQDeleteError() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).Return(output, nil)

	dlqName := "HEDWIG-DEV-MYAPP-DLQ"
	dlqURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + dlqName
	queueInput = &sqs.GetQueueUrlInput{
		QueueName: &dlqName,
	}
	output = &sqs.GetQueueUrlOutput{
		QueueUrl: &dlqURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).
		Return(output, nil).
		Once()

	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(dlqURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	receiveCount := 1
	body := `{"vehicle_id": "C_123"}`
	messageID := "123"
	receiptHandle := "foobar"
	sqsMessage := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo": {StringValue: aws.String("bar")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String("1295500510456"),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String("1295500510123"),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(strconv.Itoa(int(receiveCount))),
		},
		Body:      aws.String(body),
		MessageId: aws.String(messageID),
	}
	receiveOutput := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{&sqsMessage},
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(receiveOutput, nil).
		Once().
		After(time.Millisecond * 10)
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil)

	sendInput1 := sqs.SendMessageBatchRequestEntry{
		Id:                aws.String(messageID),
		MessageAttributes: sqsMessage.MessageAttributes,
		MessageBody:       aws.String(body),
	}
	sendInput := &sqs.SendMessageBatchInput{
		Entries:  []*sqs.SendMessageBatchRequestEntry{&sendInput1},
		QueueUrl: aws.String(queueURL),
	}
	sendOutput := &sqs.SendMessageBatchOutput{
		Failed: []*sqs.BatchResultErrorEntry{},
		Successful: []*sqs.SendMessageBatchResultEntry{
			{Id: aws.String(messageID)},
		},
	}
	s.fakeSQS.On("SendMessageBatchWithContext", mock.AnythingOfType("*context.timerCtx"), sendInput, mock.Anything).
		Return(sendOutput, nil).
		Once()

	deleteInput := &sqs.DeleteMessageBatchInput{
		Entries: []*sqs.DeleteMessageBatchRequestEntry{
			{
				Id:            aws.String(messageID),
				ReceiptHandle: aws.String(receiptHandle),
			},
		},
		QueueUrl: aws.String(dlqURL),
	}

	s.fakeSQS.On("DeleteMessageBatchWithContext", mock.AnythingOfType("*context.timerCtx"), deleteInput, []request.Option(nil)).
		Return((*sqs.DeleteMessageBatchOutput)(nil), errors.New("no internet")).
		Once()

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*1)
	defer cancel()

	testutils.RunAndWait(func() {
		err := s.backend.RequeueDLQ(ctx, numMessages, visibilityTimeout)
		s.EqualError(err, "failed to ack messages: no internet")
	})

	s.fakeSQS.AssertExpectations(s.T())
	s.fakeConsumerCallback.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestRequeueDLQDeletePartialError() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).Return(output, nil)

	dlqName := "HEDWIG-DEV-MYAPP-DLQ"
	dlqURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + dlqName
	queueInput = &sqs.GetQueueUrlInput{
		QueueName: &dlqName,
	}
	output = &sqs.GetQueueUrlOutput{
		QueueUrl: &dlqURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", mock.AnythingOfType("*context.timerCtx"), queueInput, mock.Anything).
		Return(output, nil).
		Once()

	receiveInput := &sqs.ReceiveMessageInput{
		QueueUrl:              aws.String(dlqURL),
		AttributeNames:        []*string{aws.String(sqs.QueueAttributeNameAll)},
		MessageAttributeNames: []*string{aws.String(sqs.QueueAttributeNameAll)},
		VisibilityTimeout:     aws.Int64(int64(visibilityTimeout.Seconds())),
		MaxNumberOfMessages:   aws.Int64(int64(numMessages)),
		WaitTimeSeconds:       aws.Int64(sqsWaitTimeoutSeconds),
	}
	receiveCount := 1
	body := `{"vehicle_id": "C_123"}`
	messageID := "123"
	receiptHandle := "foobar"
	sqsMessage := sqs.Message{
		ReceiptHandle: aws.String(receiptHandle),
		MessageAttributes: map[string]*sqs.MessageAttributeValue{
			"foo": {StringValue: aws.String("bar")},
		},
		Attributes: map[string]*string{
			sqs.MessageSystemAttributeNameApproximateFirstReceiveTimestamp: aws.String("1295500510456"),
			sqs.MessageSystemAttributeNameSentTimestamp:                    aws.String("1295500510123"),
			sqs.MessageSystemAttributeNameApproximateReceiveCount:          aws.String(strconv.Itoa(int(receiveCount))),
		},
		Body:      aws.String(body),
		MessageId: aws.String(messageID),
	}
	receiveOutput := &sqs.ReceiveMessageOutput{
		Messages: []*sqs.Message{&sqsMessage},
	}
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(receiveOutput, nil).
		Once().
		After(time.Millisecond * 10)
	s.fakeSQS.On("ReceiveMessageWithContext", mock.AnythingOfType("*context.timerCtx"), receiveInput, []request.Option(nil)).
		Return(&sqs.ReceiveMessageOutput{}, nil)

	sendInput1 := sqs.SendMessageBatchRequestEntry{
		Id:                aws.String(messageID),
		MessageAttributes: sqsMessage.MessageAttributes,
		MessageBody:       aws.String(body),
	}
	sendInput := &sqs.SendMessageBatchInput{
		Entries:  []*sqs.SendMessageBatchRequestEntry{&sendInput1},
		QueueUrl: aws.String(queueURL),
	}
	sendOutput := &sqs.SendMessageBatchOutput{
		Failed: []*sqs.BatchResultErrorEntry{},
		Successful: []*sqs.SendMessageBatchResultEntry{
			{Id: aws.String(messageID)},
		},
	}
	s.fakeSQS.On("SendMessageBatchWithContext", mock.AnythingOfType("*context.timerCtx"), sendInput, mock.Anything).
		Return(sendOutput, nil).
		Once()

	deleteInput := &sqs.DeleteMessageBatchInput{
		Entries: []*sqs.DeleteMessageBatchRequestEntry{
			{
				Id:            aws.String(messageID),
				ReceiptHandle: aws.String(receiptHandle),
			},
		},
		QueueUrl: aws.String(dlqURL),
	}
	deleteOutput := &sqs.DeleteMessageBatchOutput{
		Failed: []*sqs.BatchResultErrorEntry{
			{Id: aws.String(messageID)},
		},
		Successful: []*sqs.DeleteMessageBatchResultEntry{},
	}

	s.fakeSQS.On("DeleteMessageBatchWithContext", mock.AnythingOfType("*context.timerCtx"), deleteInput, []request.Option(nil)).
		Return(deleteOutput, nil).
		Once()

	ctx, cancel := context.WithTimeout(ctx, time.Millisecond*1)
	defer cancel()

	testutils.RunAndWait(func() {
		err := s.backend.RequeueDLQ(ctx, numMessages, visibilityTimeout)
		s.EqualError(err, "failed to ack some messages")
	})

	s.fakeSQS.AssertExpectations(s.T())
	s.fakeConsumerCallback.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestRequeueDLQGetQueueError() {
	ctx := context.Background()
	numMessages := uint32(10)
	visibilityTimeout := time.Second * 10
	queueName := "HEDWIG-DEV-MYAPP"
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).
		Return((*sqs.GetQueueUrlOutput)(nil), errors.New("no internet"))

	err := s.backend.RequeueDLQ(ctx, numMessages, visibilityTimeout)
	s.EqualError(err, "failed to get SQS Queue URL: no internet")

	s.fakeSQS.AssertExpectations(s.T())
	s.fakeConsumerCallback.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestPublish() {
	ctx := context.Background()

	msgTopic := "dev-myapp"
	expectedTopic := s.backend.getSNSTopic(msgTopic)

	attributes := map[string]*sns.MessageAttributeValue{
		"foo": {
			DataType:    aws.String("String"),
			StringValue: aws.String("bar"),
		},
	}

	expectedSnsInput := &sns.PublishInput{
		TopicArn:          &expectedTopic,
		Message:           aws.String(string(s.payload)),
		MessageAttributes: attributes,
	}
	output := &sns.PublishOutput{
		MessageId: aws.String("123"),
	}

	s.fakeSNS.On("PublishWithContext", ctx, expectedSnsInput, mock.Anything).
		Return(output, nil)

	messageID, err := s.backend.Publish(ctx, s.message, s.payload, s.attributes, msgTopic)
	s.NoError(err)
	s.Equal(messageID, "123")

	s.fakeSNS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestPublishInvalidCharacters() {
	ctx := context.Background()

	msgTopic := "dev-myapp"
	expectedTopic := s.backend.getSNSTopic(msgTopic)

	attributes := map[string]*sns.MessageAttributeValue{
		"foo": {
			DataType:    aws.String("String"),
			StringValue: aws.String("bar"),
		},
		"hedwig_encoding": {
			DataType:    aws.String("String"),
			StringValue: aws.String("base64"),
		},
	}

	invalidPayload := []byte("\x19")

	expectedSnsInput := &sns.PublishInput{
		TopicArn:          &expectedTopic,
		Message:           aws.String(base64.StdEncoding.EncodeToString(invalidPayload)),
		MessageAttributes: attributes,
	}
	output := &sns.PublishOutput{
		MessageId: aws.String("123"),
	}

	s.fakeSNS.On("PublishWithContext", ctx, expectedSnsInput, mock.Anything).
		Return(output, nil)

	messageID, err := s.backend.Publish(ctx, s.message, invalidPayload, s.attributes, msgTopic)
	s.NoError(err)
	s.Equal(messageID, "123")

	s.fakeSNS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestPublishFailure() {
	ctx := context.Background()

	msgTopic := "dev-myapp"
	expectedTopic := s.backend.getSNSTopic(msgTopic)

	attributes := map[string]*sns.MessageAttributeValue{
		"foo": {
			DataType:    aws.String("String"),
			StringValue: aws.String("bar"),
		},
	}

	expectedSnsInput := &sns.PublishInput{
		TopicArn:          &expectedTopic,
		Message:           aws.String(string(s.payload)),
		MessageAttributes: attributes,
	}

	s.fakeSNS.On("PublishWithContext", ctx, expectedSnsInput, mock.Anything).
		Return((*sns.PublishOutput)(nil), errors.New("failed"))

	_, err := s.backend.Publish(ctx, s.message, s.payload, s.attributes, msgTopic)
	s.EqualError(err, "Failed to publish message to SNS: failed")

	s.fakeSNS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestAck() {
	ctx := context.Background()

	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).Return(output, nil)

	receiptHandle := "foobar"

	deleteInput := &sqs.DeleteMessageInput{
		QueueUrl:      &queueURL,
		ReceiptHandle: aws.String(receiptHandle),
	}
	deleteOutput := &sqs.DeleteMessageOutput{}

	s.fakeSQS.On("DeleteMessageWithContext", ctx, deleteInput, mock.Anything).
		Return(deleteOutput, nil)

	err := s.backend.AckMessage(ctx, Metadata{ReceiptHandle: receiptHandle})
	s.NoError(err)

	s.fakeSQS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestAckError() {
	ctx := context.Background()

	queueName := "HEDWIG-DEV-MYAPP"
	queueURL := "https://sqs.us-east-1.amazonaws.com/686176732873/" + queueName
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	output := &sqs.GetQueueUrlOutput{
		QueueUrl: &queueURL,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).Return(output, nil)

	receiptHandle := "foobar"

	deleteInput := &sqs.DeleteMessageInput{
		QueueUrl:      &queueURL,
		ReceiptHandle: aws.String(receiptHandle),
	}

	s.fakeSQS.On("DeleteMessageWithContext", ctx, deleteInput, mock.Anything).
		Return((*sqs.DeleteMessageOutput)(nil), errors.New("failed to ack"))

	err := s.backend.AckMessage(ctx, Metadata{ReceiptHandle: receiptHandle})
	s.EqualError(err, "failed to ack")

	s.fakeSQS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestAckGetQueueError() {
	ctx := context.Background()

	queueName := "HEDWIG-DEV-MYAPP"
	queueInput := &sqs.GetQueueUrlInput{
		QueueName: &queueName,
	}
	s.fakeSQS.On("GetQueueUrlWithContext", ctx, queueInput, mock.Anything).
		Return((*sqs.GetQueueUrlOutput)(nil), errors.New("no internet"))

	receiptHandle := "foobar"

	err := s.backend.AckMessage(ctx, Metadata{ReceiptHandle: receiptHandle})
	s.EqualError(err, "failed to get SQS Queue URL: no internet")

	s.fakeSQS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestNack() {
	ctx := context.Background()

	receiptHandle := "foobar"

	err := s.backend.NackMessage(ctx, Metadata{ReceiptHandle: receiptHandle})
	s.NoError(err)

	// no calls expected
	s.fakeSQS.AssertExpectations(s.T())
}

func (s *BackendTestSuite) TestNew() {
	assert.NotNil(s.T(), s.backend)
}

type BackendTestSuite struct {
	suite.Suite
	backend              *backend
	settings             *hedwig.Settings
	fakeSQS              *fakeSQS
	fakeSNS              *fakeSNS
	message              *hedwig.Message
	payload              []byte
	attributes           map[string]string
	validator            *fakeValidator
	fakeConsumerCallback *fakeConsumerCallback
	logger               *fakeLogger
}

func (s *BackendTestSuite) SetupTest() {
	logger := &fakeLogger{}
	settings := &hedwig.Settings{
		AWSRegion:    "us-east-1",
		AWSAccountID: "1234567890",
		QueueName:    "DEV-MYAPP",
		MessageRouting: map[hedwig.MessageTypeMajorVersion]string{
			{
				MessageType:  "user-created",
				MajorVersion: 1,
			}: "dev-user-created-v1",
		},
		GetLogger: func(ctx context.Context) hedwig.ILogger {
			return logger
		},
		ShutdownTimeout: time.Second * 10,
	}
	fakeSQS := &fakeSQS{}
	fakeSNS := &fakeSNS{}
	fakeMessageCallback := &fakeConsumerCallback{}
	message, err := hedwig.NewMessage(settings, "user-created", "1.0", map[string]string{"foo": "bar"}, &fakeHedwigDataField{})
	require.NoError(s.T(), err)

	validator := &fakeValidator{}

	payload := []byte(`{"vehicle_id": "C_123"}`)
	attributes := map[string]string{"foo": "bar"}

	s.backend = NewBackend(settings, NewAWSSessionsCache()).(*backend)
	s.backend.sqs = fakeSQS
	s.backend.sns = fakeSNS
	s.settings = settings
	s.fakeSQS = fakeSQS
	s.fakeSNS = fakeSNS
	s.message = message
	s.validator = validator
	s.payload = payload
	s.attributes = attributes
	s.fakeConsumerCallback = fakeMessageCallback
	s.logger = logger
}

func TestBackendTestSuite(t *testing.T) {
	suite.Run(t, &BackendTestSuite{})
}
